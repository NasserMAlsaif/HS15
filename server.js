// ==================== HEADSHOOTER SERVER ====================
// Multiplayer server for HeadShooter v15
// Handles rooms, authoritative game logic, and real-time synchronization

const express = require('express');
const http = require('http');
const socketIO = require('socket.io');
const path = require('path');
const crypto = require('crypto');
const fs = require('fs');

const app = express();
const server = http.createServer(app);
const io = socketIO(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

const PORT = process.env.PORT || 3000;
const IS_DEV_MODE = process.env.NODE_ENV !== 'production';
app.use(express.json());

function envInt(name, fallback, min, max) {
    const raw = Number(process.env[name]);
    if (!Number.isFinite(raw)) return fallback;
    const v = Math.floor(raw);
    return Math.max(min, Math.min(max, v));
}

const ANTI_CHEAT_MODE = String(process.env.ANTI_CHEAT_MODE || 'observe').toLowerCase() === 'enforce'
    ? 'enforce'
    : 'observe';
const DATA_DIR = path.join(__dirname, 'data');
const ANTI_CHEAT_RECENT_FILE = path.join(DATA_DIR, 'anti-cheat-recent.jsonl');
const ANTI_CHEAT_ESCALATIONS_FILE = path.join(DATA_DIR, 'anti-cheat-escalations.jsonl');
const ANTI_CHEAT_SNAPSHOTS_FILE = path.join(DATA_DIR, 'anti-cheat-room-snapshots.jsonl');

// ==================== GAME CONSTANTS ====================
const MAP_WIDTH = 3000;
const MAP_HEIGHT = 2000;
const TICK_RATE = 30;
const TICK_MS = 1000 / TICK_RATE;
const BASE_SPEED_PER_SEC = 127.05; // 2.1175 * 60fps
const MAX_PLAYERS_PER_ROOM = 6;
const BOT_COUNT = 5;
const GAME_DURATION = 110; // 1 minute 50 seconds
const PLAYER_RESPAWN_DELAY = 3000;
const BUFF_RESPAWN_DELAY = 6000;
const KILL_CHAIN_WINDOW = 6000;
const COUNTDOWN_DURATION = 3000;
const MIN_SHOT_INTERVAL_MS = 140; // server-side fire rate cap
const BASE_CHARGE_REQUIRED_MS = 1000;
const FAST_CHARGE_REQUIRED_MS = 850;
const CHARGE_VALIDATION_GRACE_MS = 90;
const SHOT_ORIGIN_TOLERANCE = 6;
const SHOT_PATH_SAMPLE_STEP = 6;
const MAX_ACTIVE_PROJECTILES_PER_PLAYER = 8;
const MAX_INPUT_AHEAD_SEQ = 200;
const MAX_INPUT_STALE_MS = 4000;
const MAX_INPUT_SEQ_VALUE = 1000000000;
const MAX_ABS_ANGLE = Math.PI * 32;
const SHOT_ANGLE_WARN_DELTA = 1.8; // ~103 degrees
const SHOT_ANGLE_HARD_DELTA = 2.75; // ~157 degrees
const ANTI_CHEAT_LOG_COOLDOWN_MS = 2000;
const SESSION_TTL_MS = 1000 * 60 * 60 * 24 * 14; // 14 days
const SESSION_SECRET = process.env.SESSION_SECRET || 'headshooter-dev-secret-change-me';
const MATCH_RESULT_TTL_MS = 30 * 60 * 1000;
const MAX_ANTI_CHEAT_REASON_SAMPLE = 20;
const MAX_ANTI_CHEAT_RECENT = 100;
const RECONNECT_GUARD_WINDOW_MS = 20000;
const MAX_RECONNECT_ATTEMPTS_IN_WINDOW = 6;
const INPUT_TOGGLE_WINDOW_MS = 1500;
const INPUT_TOGGLE_WARN_POINTS = 45;
const CONNECTION_IP_WINDOW_MS = 60000;
const MAX_CONNECTIONS_PER_IP_WINDOW = 90;
const REGISTER_IP_WINDOW_MS = 30000;
const MAX_REGISTER_PER_IP_WINDOW = 25;
const REGISTER_PID_WINDOW_MS = 30000;
const MAX_REGISTER_PER_PID_WINDOW = 12;
const CREATE_IP_WINDOW_MS = 30000;
const MAX_CREATE_PER_IP_WINDOW = 12;
const JOIN_IP_WINDOW_MS = 30000;
const MAX_JOIN_PER_IP_WINDOW = 25;
const JOIN_PID_WINDOW_MS = 30000;
const MAX_JOIN_PER_PID_WINDOW = 18;
const STRIKE_WINDOW_MS = 15000;
const STRIKE_WARN_THRESHOLD = envInt('ANTI_CHEAT_WARN_THRESHOLD', 3, 1, 200);
const STRIKE_SOFT_BLOCK_THRESHOLD = envInt('ANTI_CHEAT_SOFT_THRESHOLD', 6, 1, 300);
const STRIKE_HARD_BLOCK_THRESHOLD = envInt('ANTI_CHEAT_HARD_THRESHOLD', 10, 1, 500);
const SOFT_BLOCK_MS = envInt('ANTI_CHEAT_SOFT_BLOCK_MS', 3000, 200, 600000);
const HARD_BLOCK_MS = envInt('ANTI_CHEAT_HARD_BLOCK_MS', 8000, 500, 600000);
const BLOCK_LOG_COOLDOWN_MS = 1200;

const KILLSTREAK_TIERS = {
    EXTRA_CORE: 3,
    MOMENTUM: 5,
    FAST_CHARGE: 7,
    STEADY_AIM: 9,
    LEGENDARY: 12
};

const MAPS = ['forest', 'canyon', 'island'];

const MAP_CONFIGS = {
        forest: {
            name: 'Forest',
            floorColor: '#1a3a1a',
            gridColor: 'rgba(60,120,60,0.2)',
            objects: [
                // Trees (high density, avoid spawns)
                {x:400,y:400,w:60,h:60,type:'tree',style:0},
                {x:500,y:350,w:60,h:60,type:'tree',style:1},
                {x:650,y:300,w:60,h:60,type:'tree',style:2},
                {x:800,y:250,w:60,h:60,type:'tree',style:3},
                {x:2200,y:300,w:60,h:60,type:'tree',style:4},
                {x:2400,y:350,w:60,h:60,type:'tree',style:5},
                {x:2600,y:400,w:60,h:60,type:'tree',style:0},
                {x:2700,y:500,w:60,h:60,type:'tree',style:1},
                {x:350,y:600,w:60,h:60,type:'tree',style:2},
                {x:300,y:800,w:60,h:60,type:'tree',style:3},
                {x:2750,y:800,w:60,h:60,type:'tree',style:4},
                {x:2700,y:1100,w:60,h:60,type:'tree',style:5},
                {x:300,y:1200,w:60,h:60,type:'tree',style:0},
                {x:350,y:1400,w:60,h:60,type:'tree',style:1},
                {x:2750,y:1200,w:60,h:60,type:'tree',style:2},
                {x:2700,y:1500,w:60,h:60,type:'tree',style:3},
                {x:400,y:1600,w:60,h:60,type:'tree',style:4},
                {x:600,y:1700,w:60,h:60,type:'tree',style:5},
                {x:2200,y:1700,w:60,h:60,type:'tree',style:0},
                {x:2400,y:1650,w:60,h:60,type:'tree',style:1},
                {x:1000,y:800,w:60,h:60,type:'tree',style:2},
                {x:1100,y:850,w:60,h:60,type:'tree',style:3},
                {x:1200,y:900,w:60,h:60,type:'tree',style:4},
                {x:1800,y:800,w:60,h:60,type:'tree',style:5},
                {x:1900,y:850,w:60,h:60,type:'tree',style:0},
                {x:2000,y:900,w:60,h:60,type:'tree',style:1},
                {x:1000,y:1200,w:60,h:60,type:'tree',style:2},
                {x:1100,y:1150,w:60,h:60,type:'tree',style:3},
                {x:1800,y:1200,w:60,h:60,type:'tree',style:4},
                {x:1900,y:1150,w:60,h:60,type:'tree',style:5},
                // Ponds (collision, projectiles pass over)
                {x:800,y:600,width:120,height:80,type:'pond'},
                {x:2200,y:1400,width:130,height:90,type:'pond'},
                {x:1500,y:500,width:110,height:75,type:'pond'},
                // Decorative skeletons (no collision)
                {x:900,y:1000,type:'skeleton'},
                {x:1700,y:1100,type:'skeleton'},
                {x:1200,y:600,type:'skeleton'},
                {x:2100,y:800,type:'skeleton'}
            ]
        },
        canyon: {
            name: 'Canyon',
            floorColor: '#3a2a1a',
            gridColor: 'rgba(100,80,60,0.2)',
            objects: [
                // Rocks (high density)
                {x:450,y:450,w:90,h:90,type:'rock',style:0},
                {x:700,y:350,w:100,h:100,type:'rock',style:1},
                {x:900,y:300,w:85,h:85,type:'rock',style:2},
                {x:2100,y:300,w:95,h:95,type:'rock',style:3},
                {x:2350,y:350,w:90,h:90,type:'rock',style:0},
                {x:2550,y:450,w:100,h:100,type:'rock',style:1},
                {x:350,y:700,w:85,h:85,type:'rock',style:2},
                {x:300,y:950,w:90,h:90,type:'rock',style:3},
                {x:2700,y:700,w:95,h:95,type:'rock',style:0},
                {x:2750,y:950,w:100,h:100,type:'rock',style:1},
                {x:300,y:1300,w:85,h:85,type:'rock',style:2},
                {x:350,y:1500,w:90,h:90,type:'rock',style:3},
                {x:2700,y:1300,w:95,h:95,type:'rock',style:0},
                {x:2650,y:1550,w:100,h:100,type:'rock',style:1},
                {x:450,y:1650,w:85,h:85,type:'rock',style:2},
                {x:700,y:1700,w:90,h:90,type:'rock',style:3},
                {x:2100,y:1700,w:95,h:95,type:'rock',style:0},
                {x:2350,y:1650,w:100,h:100,type:'rock',style:1},
                {x:1000,y:700,w:110,h:110,type:'rock',style:2},
                {x:1200,y:750,w:95,h:95,type:'rock',style:3},
                {x:1400,y:800,w:100,h:100,type:'rock',style:0},
                {x:1600,y:750,w:90,h:90,type:'rock',style:1},
                {x:1800,y:700,w:105,h:105,type:'rock',style:2},
                {x:2000,y:750,w:95,h:95,type:'rock',style:3},
                {x:1000,y:1300,w:100,h:100,type:'rock',style:0},
                {x:1200,y:1250,w:90,h:90,type:'rock',style:1},
                {x:1400,y:1200,w:110,h:110,type:'rock',style:2},
                {x:1600,y:1250,w:95,h:95,type:'rock',style:3},
                {x:1800,y:1300,w:100,h:100,type:'rock',style:0},
                {x:2000,y:1250,w:90,h:90,type:'rock',style:1},
                // Chasms/pits (collision)
                {x:1100,y:500,width:140,height:90,type:'chasm'},
                {x:1900,y:1500,width:150,height:100,type:'chasm'},
                // Cacti (collision)
                {x:500,y:1000,w:40,h:60,type:'cactus'},
                {x:2500,y:1000,w:40,h:60,type:'cactus'},
                {x:1500,y:400,w:40,h:60,type:'cactus'},
                {x:1500,y:1600,w:40,h:60,type:'cactus'}
            ]
        },
        island: {
            name: 'Island',
            floorColor: '#1a4d4d',  // Deep tropical teal
            gridColor: 'rgba(80,140,140,0.2)',
            objects: [
                {x:1500,y:1000,width:500,height:350,type:'lake'},
                {x:500,y:500,width:300,height:200,type:'lake'},
                {x:2500,y:500,width:300,height:200,type:'lake'},
                {x:500,y:1500,width:300,height:200,type:'lake'},
                {x:2500,y:1500,width:300,height:200,type:'lake'},
                {x:900,y:400,w:60,h:60,type:'tree',style:0},
                {x:1000,y:350,w:60,h:60,type:'tree',style:1},
                {x:2100,y:350,w:60,h:60,type:'tree',style:2},
                {x:2000,y:400,w:60,h:60,type:'tree',style:3},
                {x:900,y:1650,w:60,h:60,type:'tree',style:4},
                {x:1000,y:1700,w:60,h:60,type:'tree',style:5},
                {x:2000,y:1650,w:60,h:60,type:'tree',style:0},
                {x:2100,y:1700,w:60,h:60,type:'tree',style:1},
                {x:1000,y:800,w:80,h:80,type:'rock',style:0},
                {x:1200,y:850,w:85,h:85,type:'rock',style:1},
                {x:1800,y:850,w:90,h:90,type:'rock',style:2},
                {x:2000,y:800,w:80,h:80,type:'rock',style:3},
                {x:1000,y:1200,w:85,h:85,type:'rock',style:0},
                {x:1200,y:1150,w:80,h:80,type:'rock',style:1},
                {x:1800,y:1150,w:90,h:90,type:'rock',style:2},
                {x:2000,y:1200,w:85,h:85,type:'rock',style:3}
            ]
        }
    };

const PLAYER_SPAWNS = [
    {id:'P1',x:200,y:200}, {id:'P2',x:2800,y:200}, {id:'P3',x:200,y:1800},
    {id:'P4',x:2800,y:1800}, {id:'P5',x:1500,y:150}, {id:'P6',x:1500,y:1850}
];

const BUFF_SPAWNS = [
    {id:'B1',x:600,y:600}, {id:'B2',x:2400,y:600}, {id:'B3',x:600,y:1400},
    {id:'B4',x:2400,y:1400}, {id:'B5',x:300,y:1000}, {id:'B6',x:2700,y:1000}
];

const BUFF_TYPES = ['health', 'shield', 'invis', 'speed'];

// ==================== ROOM MANAGEMENT ====================
const rooms = {};
const sessions = {}; // persistentId -> { token, name, exp }
const pendingMatchResults = {}; // persistentId -> { roomCode, players, endedAt, exp }
const antiCheatMetrics = {
    startedAt: Date.now(),
    totalSuspiciousEvents: 0,
    reasons: {},
    players: {},
    recent: [],
    timeline: [],
    enforcements: {
        warn: 0,
        softBlock: 0,
        hardBlock: 0,
        wouldSoftBlock: 0,
        wouldHardBlock: 0,
        blockedEvents: 0
    },
    escalations: []
};
const reconnectGuard = {}; // persistentId -> { start, count }
const handshakeGuards = {
    connectionByIp: {},
    registerByIp: {},
    registerByPid: {},
    createByIp: {},
    joinByIp: {},
    joinByPid: {}
};

function ensureDataDir() {
    try {
        if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    } catch (e) {
        console.warn('[anti-cheat] failed to ensure data dir', e && e.message ? e.message : e);
    }
}

function appendJsonl(filePath, obj) {
    try {
        ensureDataDir();
        fs.appendFileSync(filePath, JSON.stringify(obj) + '\n', 'utf8');
    } catch (e) {
        console.warn('[anti-cheat] append jsonl failed', filePath, e && e.message ? e.message : e);
    }
}

function readJsonl(filePath, limit, filterFn) {
    try {
        if (!fs.existsSync(filePath)) return [];
        const raw = fs.readFileSync(filePath, 'utf8');
        if (!raw) return [];
        const lines = raw.trim().split('\n');
        const max = Math.max(1, Math.min(2000, Number(limit) || 200));
        const start = Math.max(0, lines.length - Math.max(max * 4, max));
        const out = [];
        for (let i = start; i < lines.length; i++) {
            const line = lines[i].trim();
            if (!line) continue;
            try {
                const obj = JSON.parse(line);
                if (!filterFn || filterFn(obj)) out.push(obj);
            } catch (_) {}
        }
        return out.slice(-max);
    } catch (_) {
        return [];
    }
}

function normalizeIp(raw) {
    if (!raw || typeof raw !== 'string') return 'unknown';
    const first = raw.split(',')[0].trim();
    return first.replace(/^::ffff:/, '') || 'unknown';
}

function allowWindowCounter(store, key, windowMs, maxCount) {
    const now = Date.now();
    const safeKey = key || 'unknown';
    const bucket = store[safeKey] || { start: now, count: 0 };
    if (now - bucket.start > windowMs) {
        bucket.start = now;
        bucket.count = 0;
    }
    bucket.count += 1;
    store[safeKey] = bucket;
    return bucket.count <= maxCount;
}

function recordSuspicionTimeline(now) {
    antiCheatMetrics.timeline.push(now);
    const cutoff = now - 60000;
    while (antiCheatMetrics.timeline.length && antiCheatMetrics.timeline[0] < cutoff) {
        antiCheatMetrics.timeline.shift();
    }
}

function pushAntiCheatRecent(event) {
    antiCheatMetrics.recent.push(event);
    if (antiCheatMetrics.recent.length > MAX_ANTI_CHEAT_RECENT) {
        antiCheatMetrics.recent.shift();
    }
    appendJsonl(ANTI_CHEAT_RECENT_FILE, event);
}

function pushAntiCheatEscalation(event) {
    antiCheatMetrics.escalations.push(event);
    if (antiCheatMetrics.escalations.length > MAX_ANTI_CHEAT_RECENT) {
        antiCheatMetrics.escalations.shift();
    }
    appendJsonl(ANTI_CHEAT_ESCALATIONS_FILE, event);
}

function buildRoomSnapshot(now) {
    return Object.entries(rooms).map(([roomCode, room]) => ({
        roomCode,
        state: room.state,
        players: Object.keys(room.players || {}).length,
        antiCheatEvents: room.antiCheatEvents || 0,
        antiCheatScore: Number((room.antiCheatScore || 0).toFixed(2)),
        blockedNow: Object.values(room.players || {}).filter((p) =>
            p && p.antiCheatState && p.antiCheatState.blockUntil && p.antiCheatState.blockUntil > now
        ).length,
        escalatedPlayers: Object.values(room.players || {})
            .filter((p) => p && p.antiCheatState && p.antiCheatState.windowStrikes > 0)
            .map((p) => ({
                playerId: p.id || null,
                name: p.name || 'Player',
                level: p.antiCheatState.level || 'none',
                windowStrikes: p.antiCheatState.windowStrikes || 0,
                blockUntil: p.antiCheatState.blockUntil || 0
            }))
            .sort((a, b) => b.windowStrikes - a.windowStrikes)
            .slice(0, 10),
        topRoomSuspects: Object.entries(room.antiCheatByPlayer || {})
            .sort((a, b) => b[1] - a[1])
            .slice(0, 5)
            .map(([playerKey, score]) => ({ playerKey, score: Number(score.toFixed(2)) })),
        gameStartTime: room.gameStartTime || null
    }));
}

function appendAntiCheatSnapshot(now) {
    appendJsonl(ANTI_CHEAT_SNAPSHOTS_FILE, {
        ts: now || Date.now(),
        connectedSockets: io.engine && typeof io.engine.clientsCount === 'number' ? io.engine.clientsCount : 0,
        totalSuspiciousEvents: antiCheatMetrics.totalSuspiciousEvents,
        suspiciousEventsPerMinute: antiCheatMetrics.timeline.length,
        rooms: buildRoomSnapshot(now || Date.now())
    });
}

function ensurePlayerAntiCheatState(player, now) {
    const t = now || Date.now();
    if (!player.antiCheatState) {
        player.antiCheatState = {
            windowStart: t,
            windowStrikes: 0,
            warned: false,
            level: 'none',
            blockUntil: 0,
            lastBlockLogAt: 0,
            lastAction: null,
            lastActionAt: 0
        };
    }
    const st = player.antiCheatState;
    if (!st.windowStart || (t - st.windowStart) > STRIKE_WINDOW_MS) {
        st.windowStart = t;
        st.windowStrikes = 0;
        st.warned = false;
        st.level = 'none';
        st.blockUntil = 0;
    }
    return st;
}

function findRoomPlayerEntry(room, playerKey) {
    if (!room || !playerKey) return null;
    if (room.players[playerKey]) return [playerKey, room.players[playerKey]];
    let entry = Object.entries(room.players).find(([, p]) => p && p.id === playerKey);
    if (entry) return entry;
    entry = Object.entries(room.players).find(([, p]) => p && p.persistentId === playerKey);
    if (entry) return entry;
    entry = Object.entries(room.players).find(([, p]) => p && p.name === playerKey);
    return entry || null;
}

function applySimulatedSuspicion(roomCode, playerKey, reason, count) {
    const room = rooms[roomCode];
    if (!room) return { ok: false, message: 'Room not found' };
    const entry = findRoomPlayerEntry(room, playerKey);
    if (!entry) return { ok: false, message: 'Player not found in room' };
    const [resolvedKey, player] = entry;
    const loops = Math.max(1, Math.min(50, Number(count) || 1));
    const useReason = reason || 'simulated_suspicion';

    for (let i = 0; i < loops; i++) {
        const now = Date.now();
        const st = ensurePlayerAntiCheatState(player, now);
        player.antiCheatStrikes = (player.antiCheatStrikes || 0) + 1;
        antiCheatMetrics.totalSuspiciousEvents += 1;
        recordSuspicionTimeline(now);
        antiCheatMetrics.reasons[useReason] = (antiCheatMetrics.reasons[useReason] || 0) + 1;

        room.antiCheatEvents = (room.antiCheatEvents || 0) + 1;
        const weighted = useReason.startsWith('fire_') ? 2.0 : 1.0;
        room.antiCheatScore = (room.antiCheatScore || 0) + weighted;
        if (!room.antiCheatByPlayer) room.antiCheatByPlayer = {};
        const roomMetricKey = player.persistentId || player.id || resolvedKey;
        room.antiCheatByPlayer[roomMetricKey] = (room.antiCheatByPlayer[roomMetricKey] || 0) + weighted;

        const metricKey = player.persistentId || player.id || resolvedKey;
        if (!antiCheatMetrics.players[metricKey]) {
            antiCheatMetrics.players[metricKey] = {
                id: player.id || resolvedKey,
                persistentId: player.persistentId || null,
                name: player.name || 'Player',
                strikes: 0,
                reasons: {}
            };
        }
        antiCheatMetrics.players[metricKey].id = player.id || resolvedKey;
        antiCheatMetrics.players[metricKey].name = player.name || antiCheatMetrics.players[metricKey].name;
        antiCheatMetrics.players[metricKey].strikes += 1;
        antiCheatMetrics.players[metricKey].reasons[useReason] =
            (antiCheatMetrics.players[metricKey].reasons[useReason] || 0) + 1;

        st.windowStrikes += 1;
        let action = null;
        if (st.windowStrikes >= STRIKE_HARD_BLOCK_THRESHOLD) {
            st.level = 'hard';
            st.blockUntil = Math.max(st.blockUntil || 0, now + HARD_BLOCK_MS);
            action = 'hardBlock';
        } else if (st.windowStrikes >= STRIKE_SOFT_BLOCK_THRESHOLD) {
            if (st.level !== 'hard') st.level = 'soft';
            st.blockUntil = Math.max(st.blockUntil || 0, now + SOFT_BLOCK_MS);
            action = 'softBlock';
        } else if (st.windowStrikes >= STRIKE_WARN_THRESHOLD && !st.warned) {
            st.warned = true;
            action = 'warn';
        }
        if (action) {
            st.lastAction = action;
            st.lastActionAt = now;
            antiCheatMetrics.enforcements[action] = (antiCheatMetrics.enforcements[action] || 0) + 1;
            pushAntiCheatEscalation({
                ts: now,
                action,
                room: roomCode,
                socketId: player.id || resolvedKey,
                playerId: player.id || resolvedKey,
                name: player.name || 'Player',
                details: {
                    simulated: true,
                    reason: useReason,
                    until: st.blockUntil || 0,
                    windowStrikes: st.windowStrikes
                }
            });
        }

        pushAntiCheatRecent({
            ts: now,
            reason: useReason,
            room: roomCode,
            socketId: player.id || resolvedKey,
            playerId: player.id || resolvedKey,
            name: player.name || 'Player',
            details: {
                simulated: true,
                windowStrikes: st.windowStrikes,
                level: st.level || 'none'
            }
        });
    }

    const st = ensurePlayerAntiCheatState(player, Date.now());
    return {
        ok: true,
        roomCode,
        playerKey: resolvedKey,
        playerId: player.id || resolvedKey,
        playerName: player.name || 'Player',
        reason: useReason,
        applied: loops,
        antiCheatStrikes: player.antiCheatStrikes || 0,
        windowStrikes: st.windowStrikes || 0,
        level: st.level || 'none',
        blockUntil: st.blockUntil || 0
    };
}

function toBase64Url(str) {
    return Buffer.from(str, 'utf8').toString('base64url');
}

function fromBase64Url(b64) {
    return Buffer.from(b64, 'base64url').toString('utf8');
}

function signSessionPayload(payloadB64) {
    return crypto.createHmac('sha256', SESSION_SECRET).update(payloadB64).digest('base64url');
}

function issueSessionToken(persistentId, name) {
    const payload = {
        pid: persistentId,
        name: name || 'Player',
        exp: Date.now() + SESSION_TTL_MS,
        nonce: crypto.randomBytes(12).toString('hex')
    };
    const payloadB64 = toBase64Url(JSON.stringify(payload));
    const sig = signSessionPayload(payloadB64);
    return `${payloadB64}.${sig}`;
}

function verifySessionToken(token, persistentId) {
    if (!token || typeof token !== 'string') return null;
    const parts = token.split('.');
    if (parts.length !== 2) return null;
    const payloadB64 = parts[0];
    const givenSig = parts[1];
    const expectedSig = signSessionPayload(payloadB64);
    const a = Buffer.from(givenSig);
    const b = Buffer.from(expectedSig);
    if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return null;
    let payload;
    try {
        payload = JSON.parse(fromBase64Url(payloadB64));
    } catch (e) {
        return null;
    }
    if (!payload || payload.pid !== persistentId) return null;
    if (!payload.exp || Date.now() > payload.exp) return null;
    return payload;
}
function emitLobbyUpdate(roomCode, targetSocket = null) {
    const room = rooms[roomCode];
    if (!room) return;
    const payload = {
        roomCode: room.code,
        leaderId: room.leader,
        players: room.players,
        state: room.state
    };
    if (targetSocket) {
        targetSocket.emit('lobbyUpdate', payload);
        return;
    }
    io.to(roomCode).emit('lobbyUpdate', payload);
}

function clonePlayersForResults(room) {
    const out = {};
    Object.entries(room.players || {}).forEach(([key, p]) => {
        out[key] = {
            id: p.id,
            persistentId: p.persistentId || null,
            name: p.name || 'Player',
            x: p.x || 0,
            y: p.y || 0,
            angle: p.angle || 0,
            hp: p.hp || 0,
            maxHp: p.maxHp || 3,
            kills: p.kills || 0,
            deaths: p.deaths || 0,
            headshots: p.headshots || 0,
            killstreak: p.killstreak || 0,
            disconnected: !!p.disconnected
        };
    });
    return out;
}

function storePendingMatchResults(roomCode, playersSnapshot) {
    const now = Date.now();
    Object.values(playersSnapshot || {}).forEach((p) => {
        if (!p || !p.persistentId) return;
        pendingMatchResults[p.persistentId] = {
            roomCode,
            players: playersSnapshot,
            endedAt: now,
            exp: now + MATCH_RESULT_TTL_MS
        };
    });
}

function emitPendingMatchResultsForSocket(socket) {
    const pid = socket.authPersistentId;
    if (!pid) return false;
    const pending = pendingMatchResults[pid];
    if (!pending) return false;
    if (Date.now() > pending.exp) {
        delete pendingMatchResults[pid];
        return false;
    }
    socket.emit('matchResultsPending', {
        roomCode: pending.roomCode,
        players: pending.players,
        endedAt: pending.endedAt
    });
    return true;
}

function emitRecentRoomResultsForSocket(socket) {
    if (!socket || !socket.authPersistentId) return false;
    const pid = socket.authPersistentId;
    const now = Date.now();
    for (const room of Object.values(rooms)) {
        if (!room || !room.lastMatchResults || !room.lastMatchEndedAt) continue;
        if (now - room.lastMatchEndedAt > MATCH_RESULT_TTL_MS) continue;
        if (room.lastMatchSeen && room.lastMatchSeen[pid]) continue;
        const found = Object.values(room.lastMatchResults).some((p) => p && p.persistentId === pid);
        if (!found) continue;
        socket.emit('matchResultsPending', {
            roomCode: room.code,
            players: room.lastMatchResults,
            endedAt: room.lastMatchEndedAt
        });
        return true;
    }
    return false;
}

function resetRoomForLobby(roomCode) {
    const room = rooms[roomCode];
    if (!room) return;
    room.state = 'lobby';
    room.gameStartTime = null;
    room.projectiles = [];
    room.killChains = {};
    room.nextSpawnIndex = 0;
    room.antiCheatEvents = 0;
    room.antiCheatScore = 0;
    room.antiCheatByPlayer = {};
    Object.entries(room.players).forEach(([key, p]) => {
        p.ready = (key === room.leader);
        p.charging = false;
        p.chargeStartedAt = 0;
        p.lastShotAt = 0;
        p.inputSeq = 0;
        p.antiCheatState = {
            windowStart: 0,
            windowStrikes: 0,
            warned: false,
            level: 'none',
            blockUntil: 0,
            lastBlockLogAt: 0,
            lastAction: null,
            lastActionAt: 0
        };
        p.input = {
            w: false,
            a: false,
            s: false,
            d: false,
            angle: p.angle || 0,
            charging: false,
            seq: 0
        };
        p.inputIntegrity = { lastMask: 0, lastAt: 0, togglePoints: 0, windowStart: 0 };
    });
}

function generateRoomCode() {
    const code = Math.floor(10000 + Math.random() * 90000).toString();
    return rooms[code] ? generateRoomCode() : code;
}

function createRoom(leaderId, leaderName, leaderPersistentId) {
    const code = generateRoomCode();
    rooms[code] = {
        code: code,
        leader: leaderId,
        players: {},
        state: 'lobby', // lobby, countdown, playing, finished
        map: null,
        gameStartTime: null,
        nextSpawnIndex: 0,
        projectiles: [],
        buffs: [],
        killChains: {},
        antiCheatEvents: 0,
        antiCheatScore: 0,
        antiCheatByPlayer: {},
        lastMatchResults: null,
        lastMatchEndedAt: 0,
        lastMatchSeen: {},
        lastUpdate: Date.now()
    };
    
    // Add leader
    rooms[code].players[leaderId] = makePlayer(leaderId, leaderName, leaderPersistentId, true);
    
    return code;
}

function initializeBuffs(roomCode) {
    const room = rooms[roomCode];
    if (!room) return;
    
    room.buffs = BUFF_SPAWNS.map(spawn => ({
        id: spawn.id,
        x: spawn.x,
        y: spawn.y,
        type: BUFF_TYPES[Math.floor(Math.random() * BUFF_TYPES.length)],
        active: true,
        takenTime: 0
    }));
}

function getNextSpawn(roomCode) {
    const room = rooms[roomCode];
    if (!room) return PLAYER_SPAWNS[0];
    
    const spawn = PLAYER_SPAWNS[room.nextSpawnIndex];
    room.nextSpawnIndex = (room.nextSpawnIndex + 1) % PLAYER_SPAWNS.length;
    return spawn;
}

function isPlayerColliding(mapKey, nx, ny) {
    const map = MAP_CONFIGS[mapKey];
    if (!map || !map.objects) return false;
    for (const o of map.objects) {
        if (!['tree','rock','lake','pond','chasm','cactus'].includes(o.type)) continue;
        const ox = nx - o.x;
        const oy = ny - o.y;
        if (['lake','pond','chasm'].includes(o.type)) {
            const enx = ox / (o.width / 2);
            const eny = oy / (o.height / 2);
            if (Math.sqrt(enx * enx + eny * eny) < 1 + (18 / o.width)) return true;
        } else {
            const ow = o.w || o.width;
            if (Math.sqrt(ox * ox + oy * oy) < 18 + ow / 2) return true;
        }
    }
    return false;
}

function isProjectileBlocked(mapKey, x, y) {
    const map = MAP_CONFIGS[mapKey];
    if (!map || !map.objects) return false;
    for (const o of map.objects) {
        if (!['tree','rock','cactus'].includes(o.type)) continue;
        const ow = o.w || o.width;
        const dx = x - o.x;
        const dy = y - o.y;
        if (Math.sqrt(dx * dx + dy * dy) < ow / 2 + 3) return true;
    }
    return false;
}

function isShotPathBlocked(mapKey, fromX, fromY, toX, toY) {
    const dx = toX - fromX;
    const dy = toY - fromY;
    const dist = Math.sqrt(dx * dx + dy * dy);
    const steps = Math.max(1, Math.ceil(dist / SHOT_PATH_SAMPLE_STEP));
    for (let i = 1; i <= steps; i++) {
        const t = i / steps;
        const x = fromX + dx * t;
        const y = fromY + dy * t;
        if (isProjectileBlocked(mapKey, x, y)) return true;
    }
    return false;
}

function startGame(roomCode) {
    const room = rooms[roomCode];
    if (!room || room.state !== 'lobby') return false;
    
    // Check all players ready
    const playerList = Object.values(room.players);
    if (!playerList.every(p => p.ready)) return false;
    
    // Pick random map
    room.map = MAPS[Math.floor(Math.random() * MAPS.length)];
    room.state = 'countdown';
    
    // Initialize game state
    initializeBuffs(roomCode);
    
    // Position players at spawn points
    playerList.forEach(player => {
        const spawn = getNextSpawn(roomCode);
        player.x = spawn.x;
        player.y = spawn.y;
        player.hp = 3;
        player.maxHp = 3;
        player.kills = 0;
        player.deaths = 0;
        player.killstreak = 0;
    });
    
    // Broadcast countdown start
    io.to(roomCode).emit('countdownStart', {
        map: room.map,
        players: room.players
    });
    
    // Start game after countdown
    setTimeout(() => {
        if (rooms[roomCode]) {
            rooms[roomCode].state = 'playing';
            rooms[roomCode].gameStartTime = Date.now();
            io.to(roomCode).emit('gameStart', { startTime: room.gameStartTime });
        }
    }, COUNTDOWN_DURATION);
    
    return true;
}


// ==================== GAME LOGIC ====================
function updatePlayers(roomCode, dt) {
    const room = rooms[roomCode];
    if (!room || room.state !== 'playing') return;

    const now = Date.now();
    const mapKey = room.selectedMap || room.map || 'forest';

    Object.values(room.players).forEach(player => {
        if (player.hp <= 0) return;

        // Expire timed buffs
        if (player.hasShield && player.shieldExpire && now > player.shieldExpire) player.hasShield = false;
        if (player.invisible && player.invisExpire && now > player.invisExpire) player.invisible = false;
        if (player.speedBoost && player.speedExpire && now > player.speedExpire) player.speedBoost = false;

        // Keep disconnected players in the world, but do not process movement/input.
        // Timed buffs above must still expire while disconnected.
        if (player.disconnected) return;

        const input = player.input || { w:false,a:false,s:false,d:false,angle:player.angle,charging:false };

        let speed = BASE_SPEED_PER_SEC;
        if (player.speedBoost) speed *= 1.25;
        if (input.charging) speed *= 0.5;

        let dx = 0, dy = 0;
        const step = speed * dt;
        if (input.w) dy -= step;
        if (input.s) dy += step;
        if (input.a) dx -= step;
        if (input.d) dx += step;

        if (dx !== 0 && dy !== 0) {
            const m = Math.sqrt(dx * dx + dy * dy);
            dx = (dx / m) * step;
            dy = (dy / m) * step;
        }

        const nx = player.x + dx;
        const ny = player.y + dy;
        if (!isPlayerColliding(mapKey, nx, ny)) {
            player.x = Math.max(20, Math.min(nx, MAP_WIDTH - 20));
            player.y = Math.max(20, Math.min(ny, MAP_HEIGHT - 20));
        }
        player.angle = input.angle || player.angle;
        player.charging = !!input.charging;

        checkBuffPickup(roomCode, player.id);
    });
}

function updateProjectiles(roomCode, dt) {
    const room = rooms[roomCode];
    if (!room || room.state !== 'playing') return;
    
    const now = Date.now();
    const validProjectiles = [];
    const mapKey = room.selectedMap || room.map || 'forest';
    
    room.projectiles.forEach(proj => {
        proj.x += proj.vx * dt;
        proj.y += proj.vy * dt;
        proj.life = (proj.life || 0) + dt;
        
        // Check bounds
        if (proj.x < 0 || proj.x > MAP_WIDTH || proj.y < 0 || proj.y > MAP_HEIGHT || proj.life > 10) {
            return; // Remove projectile
        }
        
        // Check map collisions (trees/rocks/cactus)
        if (isProjectileBlocked(mapKey, proj.x, proj.y)) {
            io.to(roomCode).emit('hitEffect', {
                x: proj.x,
                y: proj.y,
                type: 'map'
            });
            return;
        }

        // Check player collisions
        let hit = false;
        Object.values(room.players).forEach(player => {
            if (player.hp <= 0 || player.id === proj.ownerId) return;
            
            const dx = proj.x - player.x;
            const dy = proj.y - player.y;
            const dist = Math.sqrt(dx * dx + dy * dy);
            
            if (dist < 21) { // Hit radius
                const isHeadshot = dist < 13;
                const blockedByShield = !!player.hasShield;
                const headshot = !blockedByShield && isHeadshot;
                const hitType = blockedByShield ? 'shield' : 'player';

                io.to(roomCode).emit('hitEffect', {
                    x: proj.x,
                    y: proj.y,
                    type: hitType,
                    headshot: headshot,
                    victimId: player.id,
                    ownerId: proj.ownerId
                });
                console.log('[srv] hitEffect', roomCode, hitType, 'headshot=', headshot, 'owner=', proj.ownerId, 'victim=', player.id);
                
                // Handle damage (shield blocks exactly one hit, even headshots)
                if (blockedByShield) {
                    player.hasShield = false;
                    io.to(roomCode).emit('shieldBreak', { playerId: player.id });
                } else {
                    player.hp = headshot ? 0 : player.hp - 1;
                    
                    if (player.hp <= 0) {
                        handleKill(roomCode, proj.ownerId, player.id, headshot);
                    }
                }
                
                hit = true;
            }
        });
        
        if (!hit) {
            validProjectiles.push(proj);
        }
    });
    
    room.projectiles = validProjectiles;
}

function handleKill(roomCode, killerId, victimId, isHeadshot) {
    const room = rooms[roomCode];
    if (!room) return;
    
    const killer = room.players[killerId];
    const victim = room.players[victimId];
    
    if (!killer || !victim) return;
    
    // Update stats
    killer.kills++;
    killer.killstreak++;
    victim.deaths++;
    victim.killstreak = 0;
    
    // Kill chain tracking
    const now = Date.now();
    if (!room.killChains[killerId]) {
        room.killChains[killerId] = { count: 0, lastKillTime: 0 };
    }
    
    const chain = room.killChains[killerId];
    if (now - chain.lastKillTime <= KILL_CHAIN_WINDOW) {
        chain.count++;
    } else {
        chain.count = 1;
    }
    chain.lastKillTime = now;
    
    // Killstreak tier check
    let killstreakTier = null;
    if (killer.killstreak === KILLSTREAK_TIERS.EXTRA_CORE) {
        killstreakTier = 'extraCore';
        killer.maxHp = 4;
        killer.hp = Math.min(killer.hp + 1, killer.maxHp);
    } else if (killer.killstreak === KILLSTREAK_TIERS.MOMENTUM) {
        killstreakTier = 'momentum';
    } else if (killer.killstreak === KILLSTREAK_TIERS.FAST_CHARGE) {
        killstreakTier = 'fastCharge';
    } else if (killer.killstreak === KILLSTREAK_TIERS.STEADY_AIM) {
        killstreakTier = 'steadyAim';
    } else if (killer.killstreak === KILLSTREAK_TIERS.LEGENDARY) {
        killstreakTier = 'legendary';
    }
    
    // Schedule respawn
    setTimeout(() => {
        if (rooms[roomCode] && rooms[roomCode].players[victimId]) {
            const spawn = getNextSpawn(roomCode);
            victim.x = spawn.x;
            victim.y = spawn.y;
            victim.hp = victim.maxHp;
            victim.maxHp = 3; // Reset Extra Core
            victim.charging = false;
            victim.chargeStartedAt = 0;
            victim.lastShotAt = 0;
            victim.inputSeq = 0;
            victim.input = { w: false, a: false, s: false, d: false, angle: victim.angle || 0, charging: false, seq: 0 };
            victim.inputIntegrity = { lastMask: 0, lastAt: 0, togglePoints: 0, windowStart: 0 };
            
            io.to(roomCode).emit('playerRespawn', {
                playerId: victimId,
                x: spawn.x,
                y: spawn.y,
                hp: victim.hp
            });
        }
    }, PLAYER_RESPAWN_DELAY);
    
    // Broadcast kill
    io.to(roomCode).emit('playerKilled', {
        killerId: killerId,
        victimId: victimId,
        isHeadshot: isHeadshot,
        killerStats: { kills: killer.kills, killstreak: killer.killstreak },
        victimStats: { deaths: victim.deaths },
        chainCount: chain.count,
        killstreakTier: killstreakTier
    });
}

function updateBuffs(roomCode) {
    const room = rooms[roomCode];
    if (!room || room.state !== 'playing') return;
    
    const now = Date.now();
    room.buffs.forEach(buff => {
        if (!buff.active && buff.takenTime > 0 && now - buff.takenTime >= BUFF_RESPAWN_DELAY) {
            buff.type = BUFF_TYPES[Math.floor(Math.random() * BUFF_TYPES.length)];
            buff.active = true;
            buff.takenTime = 0;
            
            io.to(roomCode).emit('buffRespawn', {
                buffId: buff.id,
                type: buff.type
            });
        }
    });
}

function checkBuffPickup(roomCode, playerId) {
    const room = rooms[roomCode];
    if (!room) return;
    
    const player = room.players[playerId];
    if (!player || player.hp <= 0) return;
    
    room.buffs.forEach(buff => {
        if (!buff.active) return;
        
        const dx = player.x - buff.x;
        const dy = player.y - buff.y;
        const dist = Math.sqrt(dx * dx + dy * dy);
        
        if (dist < 25) {
            buff.active = false;
            buff.takenTime = Date.now();
            
            // Apply buff
            const now = Date.now();
            if (buff.type === 'health') {
                player.hp = Math.min(player.hp + 1, player.maxHp);
            } else if (buff.type === 'shield') {
                player.hasShield = true;
                player.shieldExpire = now + 6000;
            } else if (buff.type === 'invis') {
                player.invisible = true;
                player.invisExpire = now + 6000;
            } else if (buff.type === 'speed') {
                player.speedBoost = true;
                player.speedExpire = now + 6000;
            }
            
            io.to(roomCode).emit('buffPickup', {
                playerId: playerId,
                buffId: buff.id,
                buffType: buff.type
            });
            console.log('[srv] buffPickup', roomCode, buff.type, playerId);
        }
    });
}

function makePlayer(socketId, playerName, persistentId, isLeader) {
    return {
        id: socketId,
        persistentId: persistentId || null,
        name: playerName || 'Player',
        ready: !!isLeader,
        disconnected: false,
        x: 0,
        y: 0,
        angle: 0,
        hp: 3,
        maxHp: 3,
        kills: 0,
        deaths: 0,
        killstreak: 0,
        hasShield: false,
        invisible: false,
        speedBoost: false,
        charging: false,
        chargeStartedAt: 0,
        lastShotAt: 0,
        inputSeq: 0,
        antiCheatStrikes: 0,
        antiCheatState: {
            windowStart: 0,
            windowStrikes: 0,
            warned: false,
            level: 'none',
            blockUntil: 0,
            lastBlockLogAt: 0,
            lastAction: null,
            lastActionAt: 0
        },
        input: { w: false, a: false, s: false, d: false, angle: 0, charging: false, seq: 0 },
        inputIntegrity: { lastMask: 0, lastAt: 0, togglePoints: 0, windowStart: 0 },
        lastUpdate: Date.now()
    };
}

function findPlayerEntryByPersistentId(room, persistentId) {
    if (!room || !persistentId) return null;
    const entry = Object.entries(room.players).find(([, p]) => p && p.persistentId === persistentId);
    return entry || null;
}

function normalizeAngle(angle) {
    if (!Number.isFinite(angle)) return 0;
    let a = angle;
    const full = Math.PI * 2;
    if (Math.abs(a) > MAX_ABS_ANGLE) {
        a = a % full;
    }
    while (a > Math.PI) a -= full;
    while (a < -Math.PI) a += full;
    return a;
}

function angleDeltaAbs(a, b) {
    const na = normalizeAngle(a);
    const nb = normalizeAngle(b);
    let d = Math.abs(na - nb);
    if (d > Math.PI) d = (Math.PI * 2) - d;
    return d;
}

function bitCount(v) {
    let n = v >>> 0;
    let c = 0;
    while (n) {
        c += n & 1;
        n >>>= 1;
    }
    return c;
}

// ==================== SOCKET.IO EVENTS ====================
io.on('connection', (socket) => {
    console.log(`Player connected: ${socket.id}`);
    
    let currentRoom = null;
    let heartbeatInterval = null;
    socket.playerKey = socket.id;
    socket.authPersistentId = null;
    socket.authenticated = false;
    socket.rateLimits = {};
    socket.lastAntiCheatLogAt = 0;
    socket.clientIp = normalizeIp(
        (socket.handshake && socket.handshake.headers && socket.handshake.headers['x-forwarded-for']) ||
        (socket.handshake && socket.handshake.address) ||
        (socket.conn && socket.conn.remoteAddress) ||
        ''
    );
    
    // Heartbeat
    heartbeatInterval = setInterval(() => {
        socket.emit('heartbeat');
    }, 5000);

    function pushRecentSuspicion(event) {
        pushAntiCheatRecent(event);
    }

    function registerRateLimitSuspicion(eventName) {
        const now = Date.now();
        const roomCode = socket.roomCode || null;
        antiCheatMetrics.totalSuspiciousEvents += 1;
        recordSuspicionTimeline(now);
        const reason = `rate_limit:${eventName}`;
        antiCheatMetrics.reasons[reason] = (antiCheatMetrics.reasons[reason] || 0) + 1;
        if (roomCode && rooms[roomCode]) {
            rooms[roomCode].antiCheatEvents = (rooms[roomCode].antiCheatEvents || 0) + 1;
            rooms[roomCode].antiCheatScore = (rooms[roomCode].antiCheatScore || 0) + 1.5;
        }
        pushRecentSuspicion({
            ts: now,
            reason,
            room: roomCode,
            socketId: socket.id,
            playerId: null,
            name: socket.playerName || 'Unknown',
            details: { eventName }
        });
    }

    function ensureSocketPlayerBinding(room) {
        if (!room) return null;

        // Already bound.
        if (socket.playerKey && room.players[socket.playerKey]) return socket.playerKey;

        // Direct socket id key exists.
        if (room.players[socket.id]) {
            socket.playerKey = socket.id;
            return socket.playerKey;
        }

        // Fallback by persistent identity.
        if (!socket.authPersistentId) return null;
        const entry = findPlayerEntryByPersistentId(room, socket.authPersistentId);
        if (!entry) return null;

        const [oldKey, player] = entry;
        if (room.state === 'lobby' && oldKey !== socket.id) {
            // In lobby, normalize record key to current socket.id for stable leader/actions.
            delete room.players[oldKey];
            player.id = socket.id;
            player.name = socket.playerName || player.name;
            player.disconnected = false;
            player.lastUpdate = Date.now();
            room.players[socket.id] = player;
            if (room.leader === oldKey) room.leader = socket.id;
            socket.playerKey = socket.id;
            return socket.playerKey;
        }

        socket.playerKey = oldKey;
        return socket.playerKey;
    }

    function getPlayer(room) {
        if (!room) return null;
        const key = ensureSocketPlayerBinding(room);
        if (!key) return null;
        return room.players[key] || null;
    }

    function allowEvent(eventName, maxCount, windowMs) {
        const now = Date.now();
        const rl = socket.rateLimits[eventName] || { count: 0, start: now };
        if (now - rl.start > windowMs) {
            rl.count = 0;
            rl.start = now;
        }
        rl.count += 1;
        socket.rateLimits[eventName] = rl;
        const allowed = rl.count <= maxCount;
        if (!allowed) registerRateLimitSuspicion(eventName);
        return allowed;
    }

    function ensureAntiCheatState(player, now) {
        const t = now || Date.now();
        if (!player.antiCheatState) {
            player.antiCheatState = {
                windowStart: t,
                windowStrikes: 0,
                warned: false,
                level: 'none',
                blockUntil: 0,
                lastBlockLogAt: 0,
                lastAction: null,
                lastActionAt: 0
            };
        }
        const st = player.antiCheatState;
        if (!st.windowStart || (t - st.windowStart) > STRIKE_WINDOW_MS) {
            st.windowStart = t;
            st.windowStrikes = 0;
            st.warned = false;
            st.level = 'none';
            st.blockUntil = 0;
        }
        return st;
    }

    function pushEscalation(player, action, now, details) {
        pushAntiCheatEscalation({
            ts: now,
            action,
            room: socket.roomCode || null,
            socketId: socket.id,
            playerId: player.id || socket.id,
            name: player.name || 'Player',
            details: details || {}
        });
    }

    function applySuspicionEscalation(player, reason, now) {
        const st = ensureAntiCheatState(player, now);
        st.windowStrikes += 1;
        let action = null;
        let actionDetails = null;
        const enforce = ANTI_CHEAT_MODE === 'enforce';
        let virtualUntil = 0;

        if (st.windowStrikes >= STRIKE_HARD_BLOCK_THRESHOLD) {
            st.level = 'hard';
            virtualUntil = now + HARD_BLOCK_MS;
            if (enforce) {
                st.blockUntil = Math.max(st.blockUntil || 0, virtualUntil);
            } else {
                antiCheatMetrics.enforcements.wouldHardBlock = (antiCheatMetrics.enforcements.wouldHardBlock || 0) + 1;
            }
            action = 'hardBlock';
            actionDetails = { reason, until: enforce ? (st.blockUntil || 0) : virtualUntil, windowStrikes: st.windowStrikes, mode: ANTI_CHEAT_MODE };
        } else if (st.windowStrikes >= STRIKE_SOFT_BLOCK_THRESHOLD) {
            if (st.level !== 'hard') st.level = 'soft';
            virtualUntil = now + SOFT_BLOCK_MS;
            if (enforce) {
                st.blockUntil = Math.max(st.blockUntil || 0, virtualUntil);
            } else {
                antiCheatMetrics.enforcements.wouldSoftBlock = (antiCheatMetrics.enforcements.wouldSoftBlock || 0) + 1;
            }
            action = 'softBlock';
            actionDetails = { reason, until: enforce ? (st.blockUntil || 0) : virtualUntil, windowStrikes: st.windowStrikes, mode: ANTI_CHEAT_MODE };
        } else if (st.windowStrikes >= STRIKE_WARN_THRESHOLD && !st.warned) {
            st.warned = true;
            action = 'warn';
            actionDetails = { reason, windowStrikes: st.windowStrikes, mode: ANTI_CHEAT_MODE };
        }

        if (action) {
            st.lastAction = action;
            st.lastActionAt = now;
            antiCheatMetrics.enforcements[action] = (antiCheatMetrics.enforcements[action] || 0) + 1;
            pushEscalation(player, action, now, actionDetails);
            socket.emit('antiCheatAction', {
                action,
                until: actionDetails && actionDetails.until ? actionDetails.until : (st.blockUntil || 0),
                windowStrikes: st.windowStrikes,
                mode: ANTI_CHEAT_MODE
            });
        }
    }

    function isEnforcementBlocked(player, eventName) {
        if (ANTI_CHEAT_MODE !== 'enforce') return false;
        const now = Date.now();
        const st = ensureAntiCheatState(player, now);
        if (!st.blockUntil || now >= st.blockUntil) return false;

        const blocked = (st.level === 'hard' && (eventName === 'playerInput' || eventName === 'fireProjectile')) ||
            (st.level === 'soft' && eventName === 'fireProjectile');
        if (!blocked) return false;

        if (!st.lastBlockLogAt || (now - st.lastBlockLogAt) > BLOCK_LOG_COOLDOWN_MS) {
            st.lastBlockLogAt = now;
            antiCheatMetrics.enforcements.blockedEvents += 1;
            antiCheatMetrics.totalSuspiciousEvents += 1;
            recordSuspicionTimeline(now);
            const reason = `enforcement_block:${eventName}`;
            antiCheatMetrics.reasons[reason] = (antiCheatMetrics.reasons[reason] || 0) + 1;
            pushRecentSuspicion({
                ts: now,
                reason,
                room: socket.roomCode || null,
                socketId: socket.id,
                playerId: player.id || socket.id,
                name: player.name || 'Player',
                details: { level: st.level, until: st.blockUntil }
            });
        }
        return true;
    }

    function registerSuspicion(player, reason, details) {
        if (!player) return;
        const now = Date.now();
        const st = ensureAntiCheatState(player, now);
        player.antiCheatStrikes = (player.antiCheatStrikes || 0) + 1;
        antiCheatMetrics.totalSuspiciousEvents += 1;
        recordSuspicionTimeline(now);
        antiCheatMetrics.reasons[reason] = (antiCheatMetrics.reasons[reason] || 0) + 1;
        if (socket.roomCode && rooms[socket.roomCode]) {
            const room = rooms[socket.roomCode];
            room.antiCheatEvents = (room.antiCheatEvents || 0) + 1;
            const weighted = reason.startsWith('fire_') ? 2.0 : 1.0;
            room.antiCheatScore = (room.antiCheatScore || 0) + weighted;
            const roomKey = player.persistentId || player.id || socket.id;
            if (!room.antiCheatByPlayer) room.antiCheatByPlayer = {};
            room.antiCheatByPlayer[roomKey] = (room.antiCheatByPlayer[roomKey] || 0) + weighted;
        }
        const metricKey = player.persistentId || player.id || socket.id;
        if (!antiCheatMetrics.players[metricKey]) {
            antiCheatMetrics.players[metricKey] = {
                id: player.id || socket.id,
                persistentId: player.persistentId || null,
                name: player.name || 'Player',
                strikes: 0,
                reasons: {}
            };
        }
        antiCheatMetrics.players[metricKey].id = player.id || socket.id;
        antiCheatMetrics.players[metricKey].name = player.name || antiCheatMetrics.players[metricKey].name;
        antiCheatMetrics.players[metricKey].strikes += 1;
        antiCheatMetrics.players[metricKey].reasons[reason] =
            (antiCheatMetrics.players[metricKey].reasons[reason] || 0) + 1;
        pushRecentSuspicion({
            ts: now,
            reason,
            room: socket.roomCode || null,
            socketId: socket.id,
            playerId: player.id || socket.id,
            name: player.name || 'Player',
            details: {
                ...(details || {}),
                windowStrikes: st.windowStrikes + 1,
                level: st.level || 'none'
            }
        });
        applySuspicionEscalation(player, reason, now);
        if (now - (socket.lastAntiCheatLogAt || 0) < ANTI_CHEAT_LOG_COOLDOWN_MS) return;
        socket.lastAntiCheatLogAt = now;
        console.warn('[anti-cheat]', reason, {
            room: socket.roomCode || null,
            socketId: socket.id,
            playerId: player.id,
            name: player.name,
            strikes: player.antiCheatStrikes,
            ...(details || {})
        });
    }

    if (!allowWindowCounter(handshakeGuards.connectionByIp, socket.clientIp, CONNECTION_IP_WINDOW_MS, MAX_CONNECTIONS_PER_IP_WINDOW)) {
        registerRateLimitSuspicion('connection_ip');
        socket.emit('authError', { message: 'Too many connection attempts. Please wait.' });
        socket.disconnect(true);
        return;
    }

    function resolveRoomForSocket(data) {
        const requestedRoomCode = data && typeof data.roomCode === 'string' ? data.roomCode : '';
        let roomCode = socket.roomCode || null;

        if (requestedRoomCode && rooms[requestedRoomCode]) {
            roomCode = requestedRoomCode;
        }

        if ((!roomCode || !rooms[roomCode]) && socket.authPersistentId) {
            const found = Object.entries(rooms).find(([, room]) => {
                return Object.values(room.players || {}).some(p => p && p.persistentId === socket.authPersistentId);
            });
            if (found) roomCode = found[0];
        }

        const room = roomCode ? rooms[roomCode] : null;
        if (!room) return null;

        socket.roomCode = roomCode;
        currentRoom = roomCode;
        // Ensure socket receives room broadcasts after reconnect/back-to-lobby flow.
        socket.join(roomCode);
        if (!socket.playerKey || !room.players[socket.playerKey]) {
            const keyBySocket = room.players[socket.id] ? socket.id : null;
            if (keyBySocket) {
                socket.playerKey = keyBySocket;
            } else if (socket.authPersistentId) {
                const entry = findPlayerEntryByPersistentId(room, socket.authPersistentId);
                if (entry) socket.playerKey = entry[0];
            }
        }
        ensureSocketPlayerBinding(room);
        return { roomCode, room };
    }

    function removePlayerFromRoom(options = {}) {
        const { preserveInMatch = false } = options;
        const roomCode = socket.roomCode;
        if (!roomCode || !rooms[roomCode]) return;

        const room = rooms[roomCode];
        const key = socket.playerKey || socket.id;
        const player = room.players[key];
        if (!player) return;

        const inMatch = room.state === 'playing' || room.state === 'starting' || room.state === 'countdown';
        if (preserveInMatch && inMatch) {
            player.disconnected = true;
            player.input = { w: false, a: false, s: false, d: false, angle: player.angle || 0, charging: false, seq: 0 };
            player.inputIntegrity = { lastMask: 0, lastAt: 0, togglePoints: 0, windowStart: 0 };
            player.chargeStartedAt = 0;
            socket.leave(roomCode);
            console.log(`Player ${player.name} disconnected (preserved) in room ${roomCode}`);

            if (room.leader === key) {
                const connectedPlayers = Object.values(room.players).filter(p => !p.disconnected);
                const fallback = connectedPlayers[0] || Object.values(room.players)[0];
                if (fallback) {
                    const newLeaderKey = Object.keys(room.players).find(k => room.players[k] === fallback) || fallback.id;
                    room.leader = newLeaderKey;
                    fallback.ready = true;
                    io.to(roomCode).emit('newLeader', { leaderId: room.leader, leaderName: fallback.name });
                }
            }
            return;
        }

        console.log(`Player ${player.name} left room ${roomCode}`);
        delete room.players[key];
        socket.leave(roomCode);

        const remainingPlayers = Object.values(room.players);
        if (remainingPlayers.length === 0) {
            delete rooms[roomCode];
            console.log(`Room ${roomCode} deleted (empty)`);
            return;
        }

        if (room.leader === key) {
            const newLeader = remainingPlayers[0];
            const newLeaderKey = Object.keys(room.players).find(k => room.players[k] === newLeader) || newLeader.id;
            room.leader = newLeaderKey;
            newLeader.ready = true;
            console.log(`New leader: ${newLeader.name}`);
            io.to(roomCode).emit('newLeader', { leaderId: room.leader, leaderName: newLeader.name });
        }

        emitLobbyUpdate(roomCode);
        io.to(roomCode).emit('playerLeft', {
            playerId: key,
            playerName: player ? player.name : 'Unknown'
        });
    }
    
    // داخل io.on('connection', ...) في ملف server.js
    socket.on('registerPlayer', (data) => {
        if (!allowEvent('registerPlayer', 12, 10000)) return;
        if (!allowWindowCounter(handshakeGuards.registerByIp, socket.clientIp, REGISTER_IP_WINDOW_MS, MAX_REGISTER_PER_IP_WINDOW)) {
            registerRateLimitSuspicion('register_ip');
            socket.emit('authError', { message: 'Too many register attempts. Please wait.' });
            return;
        }
        const incomingId = (data && typeof data.id === 'string') ? data.id.trim() : '';
        const incomingName = (data && typeof data.name === 'string') ? data.name.trim() : 'Player';
        const incomingToken = data && typeof data.token === 'string' ? data.token : '';
        if (incomingId && !allowWindowCounter(handshakeGuards.registerByPid, incomingId, REGISTER_PID_WINDOW_MS, MAX_REGISTER_PER_PID_WINDOW)) {
            registerRateLimitSuspicion('register_pid');
            socket.emit('authError', { message: 'Too many register attempts for this session. Please wait.' });
            return;
        }
        if (!incomingId || incomingId.length < 6 || incomingId.length > 64) {
            socket.emit('authError', { message: 'Invalid player identity.' });
            return;
        }

        socket.persistentId = incomingId;
        socket.playerName = incomingName || 'Player';

        const tokenPayload = verifySessionToken(incomingToken, incomingId);
        if (tokenPayload) {
            socket.authenticated = true;
            socket.authPersistentId = incomingId;
        } else {
            socket.authenticated = true; // first-time bootstrap from this device
            socket.authPersistentId = incomingId;
        }

        const newToken = issueSessionToken(incomingId, socket.playerName);
        sessions[incomingId] = { token: newToken, name: socket.playerName, exp: Date.now() + SESSION_TTL_MS };
        socket.emit('sessionToken', { token: newToken, exp: sessions[incomingId].exp });
        console.log(`Player recognized: ${socket.playerName} with ID: ${incomingId}`);

        // If this device has a finished match pending, show results after reconnect.
        emitPendingMatchResultsForSocket(socket) || emitRecentRoomResultsForSocket(socket);

        // Auto-reconnect to an active match by persistent ID
        if (!socket.authPersistentId) return;
        for (const [roomCode, room] of Object.entries(rooms)) {
            if (!room || room.state !== 'playing') continue;
            const entry = findPlayerEntryByPersistentId(room, socket.authPersistentId);
            if (!entry) continue;

            const now = Date.now();
            const g = reconnectGuard[socket.authPersistentId] || { start: now, count: 0 };
            if (now - g.start > RECONNECT_GUARD_WINDOW_MS) {
                g.start = now;
                g.count = 0;
            }
            g.count += 1;
            reconnectGuard[socket.authPersistentId] = g;
            if (g.count > MAX_RECONNECT_ATTEMPTS_IN_WINDOW) {
                registerRateLimitSuspicion('reconnect_guard');
                socket.emit('reconnectLimited', {
                    retryAfterMs: Math.max(1000, RECONNECT_GUARD_WINDOW_MS - (now - g.start))
                });
                return;
            }

            const [oldKey, player] = entry;
            if (!player.disconnected) continue;

            delete room.players[oldKey];
            player.id = socket.id;
            player.name = socket.playerName || player.name;
            player.disconnected = false;
            player.lastUpdate = Date.now();
            // Reconnect can restart client input sequence from 0.
            // Reset server-side input state to prevent anti-cheat false rejects.
            player.inputSeq = 0;
            player.lastShotAt = 0;
            player.input = {
                w: false,
                a: false,
                s: false,
                d: false,
                angle: player.angle || 0,
                charging: false,
                seq: 0
            };
            player.inputIntegrity = { lastMask: 0, lastAt: 0, togglePoints: 0, windowStart: 0 };
            player.chargeStartedAt = 0;
            room.players[socket.id] = player;

            if (room.leader === oldKey) room.leader = socket.id;

            socket.playerKey = socket.id;
            socket.roomCode = roomCode;
            currentRoom = roomCode;
            socket.join(roomCode);

            socket.emit('reconnectedToGame', {
                roomCode,
                mapKey: room.selectedMap || room.map || 'forest',
                players: room.players,
                startTime: room.gameStartTime || Date.now()
            });
            console.log(`Player ${player.name} reconnected to room ${roomCode}`);
            break;
        }
    });

    socket.on('pong', () => {
        if (currentRoom && rooms[currentRoom]) {
            const player = getPlayer(rooms[currentRoom]);
            if (player) player.lastUpdate = Date.now();
        }
    });

    socket.on('clientPing', (data) => {
        const payload = {
            t: data && Number.isFinite(data.t) ? data.t : 0,
            serverTime: Date.now()
        };
        socket.emit('clientPong', payload);
        socket.emit('serverPong', payload);
    });
    
    // Create room
    socket.on('createRoom', (data) => {
    if (!allowEvent('createRoom', 4, 10000)) return;
    if (!allowWindowCounter(handshakeGuards.createByIp, socket.clientIp, CREATE_IP_WINDOW_MS, MAX_CREATE_PER_IP_WINDOW)) {
        registerRateLimitSuspicion('create_room_ip');
        socket.emit('error', { message: 'Too many room create attempts. Please wait.' });
        return;
    }
    if (!socket.authenticated || !socket.authPersistentId) {
        socket.emit('authError', { message: 'Please re-authenticate.' });
        return;
    }
    // إنشاء الغرفة باستخدام الاسم القادم من المتصفح
        const persistentId = socket.authPersistentId;
        const code = createRoom(socket.id, data.playerName || 'Soldier', persistentId);
        currentRoom = code;
        socket.join(code);

        socket.roomCode = code;
        socket.playerKey = socket.id;
        
        // إرسال الرد للمتصفح (تأكدنا من تسمية roomCode لتطابق الكلاينت)
        socket.emit('roomCreated', {
            roomCode: code, 
            players: rooms[code].players,
            leaderId: rooms[code].leader
        });
        
        // طباعة اسم اللاعب في التيرمينال بدلاً من الـ ID فقط
        console.log(`Room ${code} created by: ${data.playerName || 'Soldier'}`);

        emitLobbyUpdate(code);
    });
    
    // Join room
    socket.on('joinRoom', (data) => {
        if (!allowEvent('joinRoom', 6, 10000)) return;
        if (!allowWindowCounter(handshakeGuards.joinByIp, socket.clientIp, JOIN_IP_WINDOW_MS, MAX_JOIN_PER_IP_WINDOW)) {
            registerRateLimitSuspicion('join_room_ip');
            socket.emit('joinError', { message: 'Too many join attempts. Please wait.' });
            return;
        }
        if (!socket.authenticated || !socket.authPersistentId) {
            socket.emit('authError', { message: 'Please re-authenticate.' });
            return;
        }
        if (!allowWindowCounter(handshakeGuards.joinByPid, socket.authPersistentId, JOIN_PID_WINDOW_MS, MAX_JOIN_PER_PID_WINDOW)) {
            registerRateLimitSuspicion('join_room_pid');
            socket.emit('joinError', { message: 'Too many join attempts for this session. Please wait.' });
            return;
        }
        const code = data.roomCode;
        const room = rooms[code];
        const persistentId = socket.authPersistentId;
        
        if (!room) {
            socket.emit('joinError', { message: 'Room not found' });
            return;
        }

        if (room.state !== 'lobby') {
            // Allow reconnect while the match is active
            if ((room.state === 'playing' || room.state === 'starting' || room.state === 'countdown') && persistentId) {
                const entry = findPlayerEntryByPersistentId(room, persistentId);
                if (entry) {
                    const [oldKey, player] = entry;
                    if (player.disconnected) {
                        delete room.players[oldKey];
                        player.id = socket.id;
                        player.name = data.playerName || player.name;
                        player.disconnected = false;
                        player.lastUpdate = Date.now();
                        // Reconnect can restart client input sequence from 0.
                        // Reset server-side input state to prevent anti-cheat false rejects.
                        player.inputSeq = 0;
                        player.lastShotAt = 0;
                        player.input = {
                            w: false,
                            a: false,
                            s: false,
                            d: false,
                            angle: player.angle || 0,
                            charging: false,
                            seq: 0
                        };
                        player.inputIntegrity = { lastMask: 0, lastAt: 0, togglePoints: 0, windowStart: 0 };
                        player.chargeStartedAt = 0;
                        room.players[socket.id] = player;
                        if (room.leader === oldKey) room.leader = socket.id;

                        currentRoom = code;
                        socket.join(code);
                        socket.roomCode = code;
                        socket.playerKey = socket.id;

                        socket.emit('reconnectedToGame', {
                            roomCode: code,
                            mapKey: room.selectedMap || room.map || 'forest',
                            players: room.players,
                            startTime: room.gameStartTime || Date.now()
                        });
                        console.log(`Player ${player.name} rejoined active room ${code}`);
                        return;
                    }
                }
            }
            socket.emit('joinError', { message: 'Game already started' });
            return;
        }
        
        const playerCount = Object.keys(room.players).length;
        if (playerCount >= MAX_PLAYERS_PER_ROOM) {
            socket.emit('joinError', { message: 'Room is full' });
            return;
        }
        
        currentRoom = code;
        socket.join(code);

        socket.roomCode = code;
        socket.playerKey = socket.id;
        
        room.players[socket.id] = makePlayer(socket.id, data.playerName || 'Player', persistentId, false);
        
        io.to(code).emit('playerJoined', {
            roomCode: code,
            players: room.players,
            leaderId: room.leader
        });

        emitLobbyUpdate(code);
        
        console.log(`Player ${socket.id} joined room ${code}`);
    });
    
    socket.on('playerReady', () => {
        if (!allowEvent('playerReady', 20, 10000)) return;
        const roomCode = socket.roomCode;
        const room = rooms[roomCode];
        ensureSocketPlayerBinding(room);
        
        const player = getPlayer(room);
        if (!room || !player) {
            console.log('Player not in any room');
            return;
        }

        
        // لا تسمح للقائد بتغيير ready status
        if (room.leader === (socket.playerKey || socket.id)) {
            console.log('Leader is always ready');
            return;
        }
        
        // تبديل الحالة
        player.ready = !player.ready;
        
        console.log(`Player ${player.name} ready status: ${player.ready}`);
        
        // إرسال التحديث للجميع
        io.to(roomCode).emit('updatePlayers', {
            players: room.players,
            leaderId: room.leader,
            roomCode: room.code
        });

        emitLobbyUpdate(roomCode);
    });

    // Toggle ready
    socket.on('toggleReady', () => {
        if (!allowEvent('toggleReady', 20, 10000)) return;
        if (!currentRoom || !rooms[currentRoom]) return;
        
        const player = getPlayer(rooms[currentRoom]);
        if (!player) return;
        
        player.ready = !player.ready;
        
        io.to(currentRoom).emit('playerReadyUpdate', {
            playerId: socket.id,
            ready: player.ready
        });

        emitLobbyUpdate(currentRoom);
    });
    
    // Start game (leader only)
    socket.on('startGame', () => {
        if (!allowEvent('startGame', 8, 10000)) return;
        const roomCode = socket.roomCode;
        const room = rooms[roomCode];
        ensureSocketPlayerBinding(room);
        
        if (!room) {
            socket.emit('error', { message: 'Room not found' });
            return;
        }
        
        if (room.leader !== (socket.playerKey || socket.id)) {
            socket.emit('error', { message: 'Only the leader can start' });
            return;
        }

        const allReady = Object.entries(room.players).every(([key, p]) => {
            if (p.disconnected) return false;
            if (key === room.leader) return true;
            return p.ready === true;
        });
        
        if (!allReady) {
            socket.emit('error', { message: 'Wait! All players must be READY.' });
            return;
        }
        
        const maps = ['forest', 'canyon', 'island'];
        const selectedMap = maps[Math.floor(Math.random() * maps.length)];
        
        room.state = 'starting';
        room.selectedMap = selectedMap;
        room.lastMatchResults = null;
        room.lastMatchEndedAt = 0;
        room.lastMatchSeen = {};

        room.projectiles = [];
        room.killChains = {};
        room.buffs = [];
        room.nextSpawnIndex = 0;
        initializeBuffs(roomCode);

        Object.values(room.players).forEach(player => {
            const spawn = getNextSpawn(roomCode);
            player.x = spawn.x;
            player.y = spawn.y;
            player.angle = 0;
            player.hp = 3;
            player.maxHp = 3;
            player.kills = 0;
            player.deaths = 0;
            player.killstreak = 0;
            player.hasShield = false;
            player.invisible = false;
            player.speedBoost = false;
            player.shieldExpire = 0;
            player.invisExpire = 0;
            player.speedExpire = 0;
            player.charging = false;
            player.chargeStartedAt = 0;
            player.lastShotAt = 0;
            player.inputSeq = 0;
            player.antiCheatStrikes = 0;
            player.antiCheatState = {
                windowStart: 0,
                windowStrikes: 0,
                warned: false,
                level: 'none',
                blockUntil: 0,
                lastBlockLogAt: 0,
                lastAction: null,
                lastActionAt: 0
            };
            player.input = { w:false,a:false,s:false,d:false,angle:0,charging:false,seq:0 };
            player.inputIntegrity = { lastMask: 0, lastAt: 0, togglePoints: 0, windowStart: 0 };
        });
        
        console.log(`Game starting in room ${roomCode} with map: ${selectedMap}`);
        
        io.to(roomCode).emit('gameStarting', {
            mapKey: selectedMap,
            players: room.players,
            roomCode: roomCode
        });
        
        setTimeout(() => {
            if (rooms[roomCode] && rooms[roomCode].state === 'starting') {
                rooms[roomCode].state = 'playing';
                rooms[roomCode].gameStartTime = Date.now();
                
                io.to(roomCode).emit('gameStarted', {
                    startTime: rooms[roomCode].gameStartTime
                });
            }
        }, 3000);
    });
    // Player input (authoritative movement)
    socket.on('playerInput', (data) => {
        if (!allowEvent('playerInput', 90, 1000)) return;
        const roomCode = socket.roomCode;
        const room = rooms[roomCode];
        if (!room || room.state !== 'playing') return;
        const player = getPlayer(room);
        if (!player || player.hp <= 0) return;
        if (isEnforcementBlocked(player, 'playerInput')) return;

        if (!data || typeof data !== 'object') {
            registerSuspicion(player, 'invalid_player_input_payload');
            return;
        }

        const now = Date.now();
        const seqRaw = (typeof data.seq === 'number') ? data.seq : player.inputSeq;
        const seq = Number.isFinite(seqRaw) ? Math.floor(seqRaw) : player.inputSeq;
        if (seq < 0 || seq > MAX_INPUT_SEQ_VALUE) {
            registerSuspicion(player, 'input_seq_out_of_range', { seq: seqRaw });
            return;
        }
        if (seq < player.inputSeq - 2 || seq > player.inputSeq + MAX_INPUT_AHEAD_SEQ) {
            registerSuspicion(player, 'input_seq_window_violation', { seq, expected: player.inputSeq });
            return;
        }

        const rawAngle = (typeof data.angle === 'number') ? data.angle : player.angle;
        if (!Number.isFinite(rawAngle)) {
            registerSuspicion(player, 'input_invalid_angle', { angle: data.angle });
            return;
        }
        const angle = normalizeAngle(rawAngle);

        player.inputSeq = Math.max(player.inputSeq || 0, seq);
        const inW = !!data.w;
        const inA = !!data.a;
        const inS = !!data.s;
        const inD = !!data.d;
        const inCharging = !!data.charging;

        if (!player.inputIntegrity) {
            player.inputIntegrity = { lastMask: 0, lastAt: 0, togglePoints: 0, windowStart: now };
        }
        const ii = player.inputIntegrity;
        if (!ii.windowStart || now - ii.windowStart > INPUT_TOGGLE_WINDOW_MS) {
            ii.windowStart = now;
            ii.togglePoints = 0;
        }
        const mask = (inW ? 1 : 0) | (inA ? 2 : 0) | (inS ? 4 : 0) | (inD ? 8 : 0) | (inCharging ? 16 : 0);
        if (ii.lastAt > 0) {
            const dtInput = Math.max(1, now - ii.lastAt);
            const changed = bitCount(mask ^ ii.lastMask);
            if (changed > 0) {
                let weight = 1;
                if (dtInput < 50) weight = 3;
                else if (dtInput < 100) weight = 2;
                ii.togglePoints += changed * weight;
            } else if (ii.togglePoints > 0 && dtInput > 120) {
                ii.togglePoints -= 1;
            }
        }
        if ((inW && inS) || (inA && inD)) {
            ii.togglePoints += 1;
        }
        ii.lastMask = mask;
        ii.lastAt = now;
        if (ii.togglePoints >= INPUT_TOGGLE_WARN_POINTS) {
            registerSuspicion(player, 'input_toggle_spam', {
                togglePoints: ii.togglePoints,
                windowMs: now - ii.windowStart
            });
            ii.windowStart = now;
            ii.togglePoints = 0;
        }

        const wasCharging = !!(player.input && player.input.charging);
        player.input = {
            w: inW,
            a: inA,
            s: inS,
            d: inD,
            angle: angle,
            charging: inCharging,
            seq: player.inputSeq
        };
        if (inCharging && !wasCharging) {
            player.chargeStartedAt = now;
        } else if (!inCharging) {
            player.chargeStartedAt = 0;
        }
        player.lastUpdate = now;

        if (player.invisible && player.input.charging) {
            player.invisible = false;
            player.invisExpire = 0;
        }
    });

    // Player movement
    socket.on('playerMove', () => {
        // Deprecated: movement is server-authoritative via playerInput
        return;
    });
    
    // Fire projectile
    socket.on('fireProjectile', (data) => {
        if (!allowEvent('fireProjectile', 18, 1000)) return;
        if (!currentRoom || !rooms[currentRoom] || rooms[currentRoom].state !== 'playing') return;
        
        const player = getPlayer(rooms[currentRoom]);
        if (!player || player.hp <= 0) return;
        if (isEnforcementBlocked(player, 'fireProjectile')) return;

        const now = Date.now();
        if ((now - (player.lastUpdate || 0)) > MAX_INPUT_STALE_MS) {
            registerSuspicion(player, 'shot_with_stale_input', { staleMs: now - (player.lastUpdate || 0) });
            return;
        }

        if (!data || typeof data !== 'object') {
            registerSuspicion(player, 'invalid_fire_payload');
            return;
        }

        if (player.lastShotAt && (now - player.lastShotAt) < MIN_SHOT_INTERVAL_MS) {
            registerSuspicion(player, 'fire_rate_violation', { delta: now - player.lastShotAt });
            return;
        }
        const chargeRequiredMs = (player.killstreak >= KILLSTREAK_TIERS.FAST_CHARGE)
            ? FAST_CHARGE_REQUIRED_MS
            : BASE_CHARGE_REQUIRED_MS;
        const chargeHeldMs = (player.chargeStartedAt && player.input && player.input.charging)
            ? (now - player.chargeStartedAt)
            : 0;
        if (chargeHeldMs < (chargeRequiredMs - CHARGE_VALIDATION_GRACE_MS)) {
            registerSuspicion(player, 'fire_charge_violation', {
                heldMs: chargeHeldMs,
                requiredMs: chargeRequiredMs,
                charging: !!(player.input && player.input.charging)
            });
            return;
        }

        const room = rooms[currentRoom];
        const activeOwnerProjectiles = room.projectiles.filter(p => p.ownerId === player.id).length;
        if (activeOwnerProjectiles >= MAX_ACTIVE_PROJECTILES_PER_PLAYER) {
            registerSuspicion(player, 'active_projectile_cap_exceeded', { activeOwnerProjectiles });
            return;
        }

        if (player.invisible) {
            player.invisible = false;
            player.invisExpire = 0;
        }
        
        const rawAngle = (typeof data.angle === 'number') ? data.angle : player.angle;
        if (!Number.isFinite(rawAngle)) {
            registerSuspicion(player, 'fire_invalid_angle', { angle: data.angle });
            return;
        }
        const angle = normalizeAngle(rawAngle);
        const inputAngle = player.input && Number.isFinite(player.input.angle) ? player.input.angle : player.angle;
        const delta = angleDeltaAbs(angle, inputAngle);
        if (delta > SHOT_ANGLE_WARN_DELTA) {
            registerSuspicion(player, 'fire_angle_mismatch', {
                delta: Number(delta.toFixed(3)),
                shotAngle: Number(angle.toFixed(3)),
                inputAngle: Number(normalizeAngle(inputAngle).toFixed(3))
            });
        }
        if (delta > SHOT_ANGLE_HARD_DELTA) {
            registerSuspicion(player, 'fire_angle_hard_reject', {
                delta: Number(delta.toFixed(3))
            });
            return;
        }
        const speed = 871.2; // 14.52 * 60fps
        const offset = 25;
        const mapKey = room.selectedMap || room.map || 'forest';
        const originX = player.x + Math.cos(angle) * offset;
        const originY = player.y + Math.sin(angle) * offset;
        const originDist = Math.sqrt(((originX - player.x) ** 2) + ((originY - player.y) ** 2));
        if (Math.abs(originDist - offset) > SHOT_ORIGIN_TOLERANCE) {
            registerSuspicion(player, 'fire_origin_distance_mismatch', {
                originDist: Number(originDist.toFixed(3)),
                expected: offset
            });
            return;
        }
        if (isProjectileBlocked(mapKey, originX, originY)) {
            registerSuspicion(player, 'fire_origin_blocked', {
                x: Number(originX.toFixed(1)),
                y: Number(originY.toFixed(1))
            });
            return;
        }
        if (isShotPathBlocked(mapKey, player.x, player.y, originX, originY)) {
            registerSuspicion(player, 'fire_path_blocked', {
                fromX: Number(player.x.toFixed(1)),
                fromY: Number(player.y.toFixed(1)),
                toX: Number(originX.toFixed(1)),
                toY: Number(originY.toFixed(1))
            });
            return;
        }
        const projectile = {
            id: `proj_${socket.id}_${Date.now()}`,
            ownerId: player.id,
            x: originX,
            y: originY,
            vx: Math.cos(angle) * speed,
            vy: Math.sin(angle) * speed,
            angle: angle,
            life: 0
        };
        
        player.lastShotAt = now;
        player.chargeStartedAt = 0;
        rooms[currentRoom].projectiles.push(projectile);
        
        io.to(currentRoom).emit('projectileFired', projectile);
        console.log('[srv] projectileFired', currentRoom, socket.id);
    });

    socket.on('leaveRoom', () => {
        if (!allowEvent('leaveRoom', 12, 10000)) return;
        if (!socket.roomCode) return;
        const roomCode = socket.roomCode;
        socket.leave(roomCode);
        removePlayerFromRoom();
        socket.roomCode = null;
        currentRoom = null;
    });

    socket.on('requestLobbyState', (data) => {
        if (!allowEvent('requestLobbyState', 20, 10000)) return;
        const resolved = resolveRoomForSocket(data);
        if (!resolved) return;
        emitLobbyUpdate(resolved.roomCode, socket);
    });

    socket.on('returnToLobby', (data) => {
        if (!allowEvent('returnToLobby', 20, 10000)) return;
        const resolved = resolveRoomForSocket(data);
        if (!resolved) {
            socket.emit('joinError', { message: 'Lobby not found' });
            return;
        }
        const { room, roomCode } = resolved;
        socket.emit('lobbySnapshot', {
            roomCode: room.code,
            leaderId: room.leader,
            players: room.players,
            state: room.state,
            ok: true
        });
        emitLobbyUpdate(roomCode);
    });

    socket.on('ackMatchResults', () => {
        if (!socket.authPersistentId) return;
        const pid = socket.authPersistentId;
        delete pendingMatchResults[pid];

        // Mark recent room snapshot as seen by this player to avoid re-opening results overlay.
        Object.values(rooms).forEach((room) => {
            if (!room || !room.lastMatchResults || !room.lastMatchEndedAt) return;
            const found = Object.values(room.lastMatchResults).some((p) => p && p.persistentId === pid);
            if (!found) return;
            if (!room.lastMatchSeen) room.lastMatchSeen = {};
            room.lastMatchSeen[pid] = true;
        });
    });

    socket.on('kickPlayer', (data) => {
        if (!allowEvent('kickPlayer', 8, 10000)) return;
        const roomCode = socket.roomCode;
        const room = rooms[roomCode];
        ensureSocketPlayerBinding(room);
        if (!room || room.state !== 'lobby') {
            socket.emit('error', { message: 'Kick allowed in lobby only' });
            return;
        }
        if (room.leader !== (socket.playerKey || socket.id)) {
            socket.emit('error', { message: 'Only leader can kick' });
            return;
        }

        const targetKey = data && typeof data.playerKey === 'string' ? data.playerKey : '';
        const targetId = data && typeof data.playerId === 'string' ? data.playerId : '';

        let removeKey = '';
        if (targetKey && room.players[targetKey]) {
            removeKey = targetKey;
        } else if (targetId && room.players[targetId]) {
            removeKey = targetId;
        } else if (targetId) {
            const foundEntry = Object.entries(room.players).find(([, p]) => p && p.id === targetId);
            if (foundEntry) removeKey = foundEntry[0];
        }

        if (!removeKey || removeKey === room.leader || !room.players[removeKey]) {
            socket.emit('error', { message: 'Invalid target' });
            return;
        }

        const targetPlayer = room.players[removeKey];
        delete room.players[removeKey];

        let targetSocket = io.sockets.sockets.get(removeKey) || io.sockets.sockets.get(targetId);
        if (!targetSocket && targetPlayer && targetPlayer.persistentId) {
            targetSocket = Array.from(io.sockets.sockets.values()).find((s) => s && s.authPersistentId === targetPlayer.persistentId) || null;
        }
        if (targetSocket) {
            targetSocket.leave(roomCode);
            targetSocket.roomCode = null;
            targetSocket.playerKey = targetSocket.id;
            targetSocket.authRoomCode = null;
            targetSocket.emit('kickedFromParty', { roomCode, by: socket.id });
        }

        io.to(roomCode).emit('playerLeft', {
            playerId: targetPlayer && targetPlayer.id ? targetPlayer.id : targetId,
            playerName: targetPlayer ? targetPlayer.name : 'Unknown'
        });
        emitLobbyUpdate(roomCode);
    });
    // Disconnect
   // استبدل السطر 563-589 بهذا:
socket.on('disconnect', () => {
    console.log(`Player disconnected: ${socket.id}`);
    removePlayerFromRoom({ preserveInMatch: true });
    
    if (socket.heartbeatInterval) {
        clearInterval(socket.heartbeatInterval);
    }
});
});

// ==================== GAME LOOP ====================
setInterval(() => {
    const dt = TICK_MS / 1000;
    Object.keys(rooms).forEach(roomCode => {
        const room = rooms[roomCode];
        if (room.state === 'playing') {
            updatePlayers(roomCode, dt);
            updateProjectiles(roomCode, dt);
            updateBuffs(roomCode);

            io.to(roomCode).emit('stateUpdate', {
                serverTime: Date.now(),
                remainingMs: Math.max(0, (GAME_DURATION * 1000) - (Date.now() - room.gameStartTime)),
                players: Object.values(room.players).map(p => ({
                    id: p.id,
                    name: p.name,
                    x: p.x,
                    y: p.y,
                    angle: p.angle,
                    hp: p.hp,
                    maxHp: p.maxHp,
                    kills: p.kills,
                    deaths: p.deaths,
                    killstreak: p.killstreak,
                    hasShield: p.hasShield,
                    invisible: p.invisible,
                    speedBoost: p.speedBoost,
                    shieldExpire: p.shieldExpire || 0,
                    invisExpire: p.invisExpire || 0,
                    speedExpire: p.speedExpire || 0,
                    charging: p.charging,
                    lastProcessedInput: p.input ? p.input.seq : 0
                })),
                projectiles: room.projectiles,
                buffs: room.buffs
            });
            
            // Check game time
            const elapsed = Date.now() - room.gameStartTime;
            if (elapsed >= GAME_DURATION * 1000) {
                const resultPlayers = clonePlayersForResults(room);
                const endedAt = Date.now();
                storePendingMatchResults(roomCode, resultPlayers);
                room.lastMatchResults = resultPlayers;
                room.lastMatchEndedAt = endedAt;
                resetRoomForLobby(roomCode);
                io.to(roomCode).emit('gameEnd', {
                    roomCode: roomCode,
                    players: resultPlayers,
                    endedAt
                });
                emitLobbyUpdate(roomCode);
            }
        }
    });
}, TICK_MS); // 30 ticks/sec

// ==================== SERVE CLIENT ====================
app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.all('/admin/simulate-suspicion', (req, res) => {
    if (!IS_DEV_MODE) {
        res.status(403).json({ ok: false, message: 'Disabled in production' });
        return;
    }

    const payload = req.method === 'POST' ? (req.body || {}) : {};
    const roomCode = String(req.query.roomCode || payload.roomCode || '').trim();
    let playerKey = String(req.query.playerKey || payload.playerKey || payload.playerId || '').trim();
    const reason = String(req.query.reason || payload.reason || 'simulated_suspicion').trim();
    const countRaw = Number(req.query.count || payload.count || 1);
    const count = Number.isFinite(countRaw) ? countRaw : 1;

    if (!roomCode) {
        res.status(400).json({
            ok: false,
            message: 'roomCode is required',
            usage: '/admin/simulate-suspicion?roomCode=12345&playerKey=<playerId|persistentId|name>&reason=test&count=3'
        });
        return;
    }
    if (!playerKey && rooms[roomCode]) {
        const keys = Object.keys(rooms[roomCode].players || {});
        if (keys.length === 1) playerKey = keys[0];
    }
    if (!playerKey) {
        res.status(400).json({
            ok: false,
            message: 'playerKey is required when room has multiple players',
            usage: '/admin/simulate-suspicion?roomCode=12345&playerKey=<playerId|persistentId|name>&reason=test&count=3'
        });
        return;
    }

    const result = applySimulatedSuspicion(roomCode, playerKey, reason, count);
    if (!result.ok) {
        res.status(404).json(result);
        return;
    }
    res.json(result);
});

app.get('/admin/logs', (req, res) => {
    const prettyParam = String(req.query.pretty || '').toLowerCase();
    const prettyJson = prettyParam === '1' || prettyParam === 'true' || prettyParam === 'yes';
    const format = String(req.query.format || 'json').toLowerCase();
    const typeRaw = String(req.query.type || 'all').toLowerCase().trim();
    const type = ['recent', 'escalations', 'snapshots', 'all'].includes(typeRaw) ? typeRaw : 'all';
    const limitRaw = Number(req.query.limit);
    const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(500, Math.floor(limitRaw))) : 100;
    const fromTsRaw = Number(req.query.fromTs);
    const toTsRaw = Number(req.query.toTs);
    const fromTs = Number.isFinite(fromTsRaw) ? fromTsRaw : null;
    const toTs = Number.isFinite(toTsRaw) ? toTsRaw : null;
    const filterRoomCode = typeof req.query.roomCode === 'string' ? req.query.roomCode.trim() : '';
    const filterPlayerKey = typeof req.query.playerKey === 'string' ? req.query.playerKey.trim() : '';
    const filterReason = typeof req.query.reason === 'string' ? req.query.reason.trim() : '';

    const accept = (e) => {
        if (!e || typeof e.ts !== 'number') return false;
        if (fromTs !== null && e.ts < fromTs) return false;
        if (toTs !== null && e.ts > toTs) return false;
        if (filterRoomCode && (e.room || e.roomCode || '') !== filterRoomCode) return false;
        if (filterPlayerKey) {
            const byPlayer =
                (e.playerId || '') === filterPlayerKey ||
                (e.socketId || '') === filterPlayerKey ||
                (e.playerKey || '') === filterPlayerKey ||
                (e.persistentId || '') === filterPlayerKey;
            if (!byPlayer) return false;
        }
        if (filterReason && (e.reason || '') !== filterReason) return false;
        return true;
    };

    const payload = { ok: true, mode: ANTI_CHEAT_MODE, filters: {
        type, limit, fromTs, toTs, roomCode: filterRoomCode || null, playerKey: filterPlayerKey || null, reason: filterReason || null
    } };

    if (type === 'recent' || type === 'all') payload.recent = readJsonl(ANTI_CHEAT_RECENT_FILE, limit, accept).reverse();
    if (type === 'escalations' || type === 'all') payload.escalations = readJsonl(ANTI_CHEAT_ESCALATIONS_FILE, limit, accept).reverse();
    if (type === 'snapshots' || type === 'all') payload.snapshots = readJsonl(ANTI_CHEAT_SNAPSHOTS_FILE, limit, accept).reverse();

    if (format === 'csv') {
        const rows = [];
        const pushRows = (kind, list) => {
            (list || []).forEach((e) => {
                rows.push({
                    type: kind,
                    ts: e.ts || '',
                    room: e.room || e.roomCode || '',
                    playerId: e.playerId || '',
                    socketId: e.socketId || '',
                    name: e.name || '',
                    reason: e.reason || '',
                    action: e.action || '',
                    details: e.details ? JSON.stringify(e.details) : ''
                });
            });
        };
        pushRows('recent', payload.recent);
        pushRows('escalation', payload.escalations);
        pushRows('snapshot', payload.snapshots);
        const head = ['type', 'ts', 'room', 'playerId', 'socketId', 'name', 'reason', 'action', 'details'];
        const esc = (v) => `"${String(v === undefined || v === null ? '' : v).replace(/"/g, '""')}"`;
        const csv = [head.join(',')]
            .concat(rows.map((r) => head.map((h) => esc(r[h])).join(',')))
            .join('\n');
        res.setHeader('Content-Type', 'text/csv; charset=utf-8');
        res.setHeader('Content-Disposition', 'attachment; filename="anti-cheat-logs.csv"');
        res.send(csv);
        return;
    }

    if (prettyJson) {
        res.type('application/json').send(JSON.stringify(payload, null, 2));
        return;
    }
    res.json(payload);
});

app.get('/admin/stats', (req, res) => {
    const now = Date.now();
    const prettyParam = String(req.query.pretty || '').toLowerCase();
    const prettyJson = prettyParam === '1' || prettyParam === 'true' || prettyParam === 'yes';
    const recentWindowSecRaw = Number(req.query.recentWindowSec);
    const recentWindowSec = Number.isFinite(recentWindowSecRaw)
        ? Math.max(10, Math.min(3600, Math.floor(recentWindowSecRaw)))
        : 120;
    const recentLimitRaw = Number(req.query.recentLimit);
    const recentLimit = Number.isFinite(recentLimitRaw)
        ? Math.max(1, Math.min(200, Math.floor(recentLimitRaw)))
        : 50;
    const filterRoomCode = typeof req.query.roomCode === 'string' ? req.query.roomCode.trim() : '';
    const filterPlayerKey = typeof req.query.playerKey === 'string' ? req.query.playerKey.trim() : '';
    const filterReason = typeof req.query.reason === 'string' ? req.query.reason.trim() : '';

    const recentCutoff = now - (recentWindowSec * 1000);
    const recentFiltered = antiCheatMetrics.recent
        .filter((e) => {
            if (!e || typeof e.ts !== 'number') return false;
            if (e.ts < recentCutoff) return false;
            if (filterRoomCode && (e.room || '') !== filterRoomCode) return false;
            if (filterPlayerKey && (e.playerId || '') !== filterPlayerKey && (e.socketId || '') !== filterPlayerKey) return false;
            if (filterReason && (e.reason || '') !== filterReason) return false;
            return true;
        })
        .sort((a, b) => b.ts - a.ts)
        .slice(0, recentLimit);

    const roomEntries = Object.entries(rooms);
    const activeRooms = roomEntries.length;
    const liveRooms = roomEntries.filter(([, room]) => room && room.state === 'playing').length;
    const connectedSockets = io.engine && typeof io.engine.clientsCount === 'number' ? io.engine.clientsCount : 0;
    const playersInRooms = roomEntries.reduce((sum, [, room]) => sum + Object.keys(room.players || {}).length, 0);
    const roomSummaries = buildRoomSnapshot(now);
    const topPlayers = Object.values(antiCheatMetrics.players)
        .sort((a, b) => b.strikes - a.strikes)
        .slice(0, 15);
    const topReasons = Object.entries(antiCheatMetrics.reasons)
        .sort((a, b) => b[1] - a[1])
        .slice(0, MAX_ANTI_CHEAT_REASON_SAMPLE)
        .map(([reason, count]) => ({ reason, count }));

    const payload = {
        uptimeSec: Math.floor((Date.now() - antiCheatMetrics.startedAt) / 1000),
        serverTime: Date.now(),
        connectedSockets,
        activeRooms,
        liveRooms,
        playersInRooms,
        antiCheat: {
            mode: ANTI_CHEAT_MODE,
            config: {
                strikeWindowMs: STRIKE_WINDOW_MS,
                warnThreshold: STRIKE_WARN_THRESHOLD,
                softThreshold: STRIKE_SOFT_BLOCK_THRESHOLD,
                hardThreshold: STRIKE_HARD_BLOCK_THRESHOLD,
                softBlockMs: SOFT_BLOCK_MS,
                hardBlockMs: HARD_BLOCK_MS
            },
            totalSuspiciousEvents: antiCheatMetrics.totalSuspiciousEvents,
            suspiciousEventsPerMinute: antiCheatMetrics.timeline.length,
            enforcement: antiCheatMetrics.enforcements,
            topReasons,
            topPlayers,
            recent: recentFiltered,
            recentEscalations: antiCheatMetrics.escalations.slice(-50).reverse(),
            recentFilters: {
                recentWindowSec,
                recentLimit,
                roomCode: filterRoomCode || null,
                playerKey: filterPlayerKey || null,
                reason: filterReason || null
            }
        },
        rooms: roomSummaries
    };
    if (prettyJson) {
        res.type('application/json').send(JSON.stringify(payload, null, 2));
        return;
    }
    res.json(payload);
});

app.get('/admin/dashboard', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'admin-dashboard.html'));
});

// ==================== START SERVER ====================
ensureDataDir();
setInterval(() => {
    appendAntiCheatSnapshot(Date.now());
}, 10000);

server.listen(PORT, () => {
    console.log(`
╔═══════════════════════════════════════╗
║   HEADSHOOTER SERVER RUNNING          ║
║   Port: ${PORT}                       ║
║   Ready for connections!              ║
╚═══════════════════════════════════════╝
    `);
});

// Handle shutdown
process.on('SIGINT', () => {
    console.log('\nShutting down server...');
    server.close(() => {
        console.log('Server closed');
        process.exit(0);
    });
});






