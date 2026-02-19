// ==================== HEADSHOOTER SERVER ====================
// Multiplayer server for HeadShooter v15
// Handles rooms, authoritative game logic, and real-time synchronization

const express = require('express');
const http = require('http');
const socketIO = require('socket.io');
const path = require('path');
const crypto = require('crypto');
const fs = require('fs');
const nodemailer = require('nodemailer');
const { Encoder: SocketIoPacketEncoder } = require('socket.io-parser');
const { createIdentityStore } = require('./identity-store');

const app = express();
const server = http.createServer(app);
const io = socketIO(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    },
    transports: ['websocket', 'polling'],
    perMessageDeflate: false,
    httpCompression: false
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

function envBool(name, fallback = false) {
    const raw = String(process.env[name] || '').trim().toLowerCase();
    if (!raw) return !!fallback;
    return raw === '1' || raw === 'true' || raw === 'yes' || raw === 'on';
}

const ANTI_CHEAT_MODE = String(process.env.ANTI_CHEAT_MODE || 'observe').toLowerCase() === 'enforce'
    ? 'enforce'
    : 'observe';
// Keep anti-cheat telemetry/logging enabled, but disable in-match penalties for now.
const ANTI_CHEAT_PENALTIES_ENABLED = false;
const DATA_DIR = path.join(__dirname, 'data');
const ANTI_CHEAT_RECENT_FILE = path.join(DATA_DIR, 'anti-cheat-recent.jsonl');
const ANTI_CHEAT_ESCALATIONS_FILE = path.join(DATA_DIR, 'anti-cheat-escalations.jsonl');
const ANTI_CHEAT_SNAPSHOTS_FILE = path.join(DATA_DIR, 'anti-cheat-room-snapshots.jsonl');

// ==================== GAME CONSTANTS ====================
const MAP_WIDTH = 3000;
const MAP_HEIGHT = 2000;
const TICK_RATE = envInt('TICK_RATE', 30, 10, 60);
const TICK_MS = 1000 / TICK_RATE;
const STATE_FULL_SNAPSHOT_INTERVAL_MS = envInt('STATE_FULL_SNAPSHOT_INTERVAL_MS', 1000, 250, 5000);
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
const PLAYER_BODY_RADIUS = 18;
const PLAYER_HEAD_VISUAL_RADIUS = 8;
const PROJECTILE_COLLISION_RADIUS = 3;
const PROJECTILE_TIP_OFFSET = 6;
const PROJECTILE_HIT_RADIUS = PLAYER_BODY_RADIUS + PROJECTILE_COLLISION_RADIUS; // 21
// Headshot when projectile touches the small head circle (with extra tolerance for mobile/net jitter).
const PROJECTILE_HEADSHOT_RADIUS = PLAYER_HEAD_VISUAL_RADIUS + PROJECTILE_COLLISION_RADIUS + 5; // 16
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
const AUTH_VERIFY_TTL_MS = envInt('AUTH_VERIFY_TTL_MS', 10 * 60 * 1000, 60 * 1000, 24 * 60 * 60 * 1000);
const AUTH_VERIFY_RESEND_COOLDOWN_MS = envInt('AUTH_VERIFY_RESEND_COOLDOWN_MS', 60 * 1000, 1000, 10 * 60 * 1000);
const AUTH_VERIFY_MAX_SENDS_PER_HOUR = envInt('AUTH_VERIFY_MAX_SENDS_PER_HOUR', 5, 1, 100);
const AUTH_VERIFY_MAX_ATTEMPTS = envInt('AUTH_VERIFY_MAX_ATTEMPTS', 8, 1, 100);
const AUTH_PASSWORD_MIN_LEN = envInt('AUTH_PASSWORD_MIN_LEN', 8, 6, 128);
const AUTH_PASSWORD_MAX_LEN = envInt('AUTH_PASSWORD_MAX_LEN', 72, AUTH_PASSWORD_MIN_LEN, 256);
const AUTH_SCRYPT_N = envInt('AUTH_SCRYPT_N', 16384, 1024, 1048576);
const AUTH_SCRYPT_R = envInt('AUTH_SCRYPT_R', 8, 1, 32);
const AUTH_SCRYPT_P = envInt('AUTH_SCRYPT_P', 1, 1, 16);
const AUTH_USERNAME_RE = /^[a-zA-Z0-9_]{3,24}$/;
const AUTH_EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const AUTH_SMTP_HOST = String(process.env.AUTH_SMTP_HOST || '').trim();
const AUTH_SMTP_PORT = envInt('AUTH_SMTP_PORT', 587, 1, 65535);
const AUTH_SMTP_SECURE = envBool('AUTH_SMTP_SECURE', false);
const AUTH_SMTP_USER = String(process.env.AUTH_SMTP_USER || '').trim();
const AUTH_SMTP_PASS = String(process.env.AUTH_SMTP_PASS || '');
const AUTH_EMAIL_FROM = String(process.env.AUTH_EMAIL_FROM || AUTH_SMTP_USER || '').trim();
const AUTH_ALLOW_OTP_FALLBACK = envBool('AUTH_ALLOW_OTP_FALLBACK', IS_DEV_MODE);
const AUTH_REQUIRE_PERSISTENT_STORE = envBool('AUTH_REQUIRE_PERSISTENT_STORE', !IS_DEV_MODE);
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
const ACTIVE_MATCH_STATES = new Set(['playing', 'starting', 'countdown']);
const PARTY_INVITE_TTL_MS = envInt('PARTY_INVITE_TTL_MS', 45000, 5000, 10 * 60 * 1000);
const INSTANT_RESPAWN_MATCH_CHARGES = 3;
const NET_MONITOR_ENABLED = envBool('NET_MONITOR_ENABLED', IS_DEV_MODE);
const NET_MONITOR_LOG_INTERVAL_MS = envInt('NET_MONITOR_LOG_INTERVAL_MS', 5000, 1000, 60000);

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
const identityStore = createIdentityStore();
if (identityStore && identityStore.mode) {
    console.log(`[identity] store mode: ${identityStore.mode}`);
    if (identityStore.mode === 'memory') {
        console.warn('[identity] DATABASE_URL is not set. Accounts/friends are in memory and will reset on restart/redeploy.');
    }
}
const sessions = {}; // persistentId -> { token, name, exp, profileId, friendCode, username, isGuest }
const pendingMatchResults = {}; // persistentId -> { roomCode, players, endedAt, exp }
const partyInvitesById = {}; // inviteId -> { id, roomCode, fromProfileId, fromName, toProfileId, status, createdAt, expiresAt, respondedAt }
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
const adRewardStateByPersistent = {}; // persistentId -> { instantRespawnPending, updatedAt }
const handshakeGuards = {
    connectionByIp: {},
    registerByIp: {},
    registerByPid: {},
    createByIp: {},
    joinByIp: {},
    joinByPid: {}
};
const socketIoPacketEncoder = new SocketIoPacketEncoder();
const NET_MONITOR_IGNORED_EVENTS = new Set([
    'heartbeat',
    'pong',
    'clientPing',
    'clientPong',
    'serverPong'
]);
const netMonitor = {
    rooms: {}
};

function createNetDirectionStats() {
    return {
        messages: 0,
        bytes: 0,
        categories: {
            playerInputs: { messages: 0, bytes: 0 },
            worldState: { messages: 0, bytes: 0 },
            events: { messages: 0, bytes: 0 }
        },
        events: {}
    };
}

function createNetTrafficStats() {
    return {
        in: createNetDirectionStats(),
        out: createNetDirectionStats()
    };
}

function classifyNetEventCategory(eventName) {
    if (eventName === 'playerInput') return 'playerInputs';
    if (eventName === 'stateUpdate') return 'worldState';
    return 'events';
}

function shouldTrackNetEvent(eventName) {
    if (!NET_MONITOR_ENABLED) return false;
    if (typeof eventName !== 'string' || !eventName) return false;
    return !NET_MONITOR_IGNORED_EVENTS.has(eventName);
}

function safeByteLength(value) {
    if (value === null || value === undefined) return 0;
    if (typeof value === 'string') return Buffer.byteLength(value);
    if (Buffer.isBuffer(value)) return value.length;
    if (value instanceof ArrayBuffer) return value.byteLength;
    if (ArrayBuffer.isView(value)) return value.byteLength;
    try {
        return Buffer.byteLength(String(value));
    } catch (_) {
        return 0;
    }
}

function normalizeEventArgs(args) {
    if (!Array.isArray(args)) return [];
    return args.filter((arg) => typeof arg !== 'function');
}

function estimateSocketIoPacketBytes(packet) {
    if (!packet) return 0;
    try {
        const encoded = socketIoPacketEncoder.encode(packet);
        let bytes = 0;
        for (const part of encoded) bytes += safeByteLength(part);
        // Engine.IO "message" envelope prefix.
        if (encoded.length > 0) bytes += 1;
        return bytes;
    } catch (_) {
        try {
            return Buffer.byteLength(JSON.stringify(packet.data || packet));
        } catch (_) {
            return 0;
        }
    }
}

function estimateSocketIoEventBytes(eventName, args) {
    const payload = [eventName].concat(normalizeEventArgs(args));
    return estimateSocketIoPacketBytes({ type: 2, nsp: '/', data: payload });
}

function formatBytes(value) {
    const bytes = Math.max(0, Number(value) || 0);
    if (bytes < 1024) return `${bytes.toFixed(0)} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(2)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
}

function formatRate(bytesPerSec) {
    return `${formatBytes(bytesPerSec)}/s`;
}

function ensureRoomNetMonitor(roomCode) {
    if (!NET_MONITOR_ENABLED) return null;
    if (!roomCode || !rooms[roomCode]) return null;
    if (!netMonitor.rooms[roomCode]) {
        netMonitor.rooms[roomCode] = {
            roomCode,
            matchIndex: 0,
            matchActive: false,
            matchStartedAt: 0,
            sampleStartedAt: 0,
            sample: createNetTrafficStats(),
            totals: createNetTrafficStats(),
            lastSummary: null
        };
    }
    return netMonitor.rooms[roomCode];
}

function appendTraffic(stats, direction, eventName, messageCount, byteCount) {
    const dir = stats[direction];
    if (!dir) return;
    const category = classifyNetEventCategory(eventName);
    const msgs = Math.max(0, Number(messageCount) || 0);
    const bytes = Math.max(0, Number(byteCount) || 0);
    dir.messages += msgs;
    dir.bytes += bytes;
    dir.categories[category].messages += msgs;
    dir.categories[category].bytes += bytes;
    if (!dir.events[eventName]) dir.events[eventName] = { messages: 0, bytes: 0 };
    dir.events[eventName].messages += msgs;
    dir.events[eventName].bytes += bytes;
}

function beginRoomNetworkMatch(roomCode) {
    const monitor = ensureRoomNetMonitor(roomCode);
    if (!monitor) return;
    if (monitor.matchActive) {
        finishRoomNetworkMatch(roomCode, 'restarted');
    }
    const now = Date.now();
    monitor.matchIndex += 1;
    monitor.matchActive = true;
    monitor.matchStartedAt = now;
    monitor.sampleStartedAt = now;
    monitor.sample = createNetTrafficStats();
    monitor.totals = createNetTrafficStats();
    monitor.lastSummary = null;
    console.log(`[net][room ${roomCode}] monitor started for match #${monitor.matchIndex}`);
}

function recordRoomNetworkEvent(roomCode, direction, eventName, bytesPerMessage, recipients = 1) {
    if (!shouldTrackNetEvent(eventName)) return;
    const monitor = ensureRoomNetMonitor(roomCode);
    if (!monitor || !monitor.matchActive) return;
    const perMessageBytes = Math.max(0, Math.floor(Number(bytesPerMessage) || 0));
    const messageCount = Math.max(1, Math.floor(Number(recipients) || 1));
    const totalBytes = perMessageBytes * messageCount;
    appendTraffic(monitor.sample, direction, eventName, messageCount, totalBytes);
    appendTraffic(monitor.totals, direction, eventName, messageCount, totalBytes);
}

function summarizeDirectionStats(dirStats, durationSec) {
    const sec = Math.max(0.001, durationSec);
    const msgPerSec = dirStats.messages / sec;
    const bytesPerSec = dirStats.bytes / sec;
    const avgMsgBytes = dirStats.messages > 0 ? (dirStats.bytes / dirStats.messages) : 0;
    return { msgPerSec, bytesPerSec, avgMsgBytes };
}

function summarizeCategoryStats(dirStats, category, durationSec) {
    const cat = dirStats.categories[category];
    const sec = Math.max(0.001, durationSec);
    const msgPerSec = cat.messages / sec;
    const bytesPerSec = cat.bytes / sec;
    const avgMsgBytes = cat.messages > 0 ? (cat.bytes / cat.messages) : 0;
    return { msgPerSec, bytesPerSec, avgMsgBytes };
}

function topEventsLine(eventsMap, durationSec, limit = 5) {
    const sec = Math.max(0.001, Number(durationSec) || 0.001);
    const top = Object.entries(eventsMap || {})
        .sort((a, b) => b[1].bytes - a[1].bytes)
        .slice(0, Math.max(1, limit))
        .map(([name, val]) => {
            const messages = val.messages || 0;
            const bytes = val.bytes || 0;
            const mps = messages / sec;
            const avg = messages > 0 ? (bytes / messages) : 0;
            return `${name}:${formatBytes(bytes)}(${messages} msgs, ${mps.toFixed(2)}/s, avg ${avg.toFixed(1)} B)`;
        });
    return top.length ? top.join(' | ') : 'none';
}

function logRoomNetworkSample(roomCode, force = false) {
    const monitor = netMonitor.rooms[roomCode];
    if (!NET_MONITOR_ENABLED || !monitor || !monitor.matchActive) return;
    const now = Date.now();
    const elapsedMs = Math.max(1, now - monitor.sampleStartedAt);
    if (!force && elapsedMs < NET_MONITOR_LOG_INTERVAL_MS) return;
    const elapsedSec = elapsedMs / 1000;
    const inSummary = summarizeDirectionStats(monitor.sample.in, elapsedSec);
    const outSummary = summarizeDirectionStats(monitor.sample.out, elapsedSec);
    const inInput = summarizeCategoryStats(monitor.sample.in, 'playerInputs', elapsedSec);
    const outState = summarizeCategoryStats(monitor.sample.out, 'worldState', elapsedSec);
    const inEvents = summarizeCategoryStats(monitor.sample.in, 'events', elapsedSec);
    const outEvents = summarizeCategoryStats(monitor.sample.out, 'events', elapsedSec);

    console.log(
        `[net][room ${roomCode}] sample ${elapsedSec.toFixed(1)}s | ` +
        `IN ${formatRate(inSummary.bytesPerSec)} (${inSummary.msgPerSec.toFixed(1)} msg/s avg ${inSummary.avgMsgBytes.toFixed(1)} B) | ` +
        `OUT ${formatRate(outSummary.bytesPerSec)} (${outSummary.msgPerSec.toFixed(1)} msg/s avg ${outSummary.avgMsgBytes.toFixed(1)} B)`
    );
    console.log(
        `[net][room ${roomCode}] playerInput IN ${formatRate(inInput.bytesPerSec)} (${inInput.msgPerSec.toFixed(1)} msg/s avg ${inInput.avgMsgBytes.toFixed(1)} B) | ` +
        `stateUpdate OUT ${formatRate(outState.bytesPerSec)} (${outState.msgPerSec.toFixed(1)} msg/s avg ${outState.avgMsgBytes.toFixed(1)} B)`
    );
    console.log(
        `[net][room ${roomCode}] events IN ${formatRate(inEvents.bytesPerSec)} (${inEvents.msgPerSec.toFixed(1)} msg/s) | ` +
        `events OUT ${formatRate(outEvents.bytesPerSec)} (${outEvents.msgPerSec.toFixed(1)} msg/s)`
    );
    console.log(`[net][room ${roomCode}] top IN events: ${topEventsLine(monitor.sample.in.events, elapsedSec)}`);
    console.log(`[net][room ${roomCode}] top OUT events: ${topEventsLine(monitor.sample.out.events, elapsedSec)}`);

    monitor.sample = createNetTrafficStats();
    monitor.sampleStartedAt = now;
}

function finishRoomNetworkMatch(roomCode, reason = 'ended') {
    const monitor = netMonitor.rooms[roomCode];
    if (!NET_MONITOR_ENABLED || !monitor || !monitor.matchActive) return;
    logRoomNetworkSample(roomCode, true);

    const room = rooms[roomCode];
    const playersCount = room && room.players ? Math.max(1, Object.keys(room.players).length) : 1;
    const durationSec = Math.max(0.001, (Date.now() - monitor.matchStartedAt) / 1000);
    const inTotals = monitor.totals.in;
    const outTotals = monitor.totals.out;
    const totalBytes = inTotals.bytes + outTotals.bytes;
    const downlinkPerPlayerPerSec = outTotals.bytes / durationSec / playersCount;
    const matchBytesPerSec = totalBytes / durationSec;

    console.log(
        `[net][room ${roomCode}] match summary (${reason}) duration=${durationSec.toFixed(1)}s players=${playersCount} ` +
        `IN=${formatBytes(inTotals.bytes)} OUT=${formatBytes(outTotals.bytes)} TOTAL=${formatBytes(totalBytes)}`
    );
    console.log(
        `[net][room ${roomCode}] per-player downlink=${formatRate(downlinkPerPlayerPerSec)} | ` +
        `whole-match throughput=${formatRate(matchBytesPerSec)}`
    );
    console.log(
        `[net][room ${roomCode}] playerInput IN total=${formatBytes(inTotals.categories.playerInputs.bytes)} ` +
        `(${inTotals.categories.playerInputs.messages} msgs)`
    );
    console.log(
        `[net][room ${roomCode}] stateUpdate OUT total=${formatBytes(outTotals.categories.worldState.bytes)} ` +
        `(${outTotals.categories.worldState.messages} msgs)`
    );
    console.log(
        `[net][room ${roomCode}] events IN total=${formatBytes(inTotals.categories.events.bytes)} ` +
        `(${inTotals.categories.events.messages} msgs) | events OUT total=${formatBytes(outTotals.categories.events.bytes)} ` +
        `(${outTotals.categories.events.messages} msgs)`
    );
    console.log(`[net][room ${roomCode}] top IN events (match): ${topEventsLine(inTotals.events, durationSec, 8)}`);
    console.log(`[net][room ${roomCode}] top OUT events (match): ${topEventsLine(outTotals.events, durationSec, 8)}`);

    monitor.lastSummary = {
        reason,
        endedAt: Date.now(),
        durationSec,
        playersCount,
        inBytes: inTotals.bytes,
        outBytes: outTotals.bytes,
        totalBytes,
        downlinkPerPlayerPerSec,
        matchBytesPerSec
    };
    monitor.matchActive = false;
}

function resolveBroadcastRecipients(adapter, opts) {
    const recipients = new Set();
    const targetRooms = opts && opts.rooms instanceof Set ? opts.rooms : null;
    if (targetRooms && targetRooms.size > 0) {
        targetRooms.forEach((roomName) => {
            const members = adapter.rooms.get(roomName);
            if (!members) return;
            members.forEach((sid) => recipients.add(sid));
        });
    } else {
        adapter.sids.forEach((_, sid) => recipients.add(sid));
    }

    const exceptRooms = opts && opts.except instanceof Set ? opts.except : null;
    if (exceptRooms && exceptRooms.size > 0) {
        exceptRooms.forEach((roomName) => {
            const members = adapter.rooms.get(roomName);
            if (!members) return;
            members.forEach((sid) => recipients.delete(sid));
        });
    }
    return recipients;
}

function groupRecipientsByRoom(recipients) {
    const grouped = {};
    recipients.forEach((sid) => {
        const targetSocket = io.sockets.sockets.get(sid);
        if (!targetSocket || !targetSocket.roomCode || !rooms[targetSocket.roomCode]) return;
        grouped[targetSocket.roomCode] = (grouped[targetSocket.roomCode] || 0) + 1;
    });
    return grouped;
}

function trackBroadcastPacket(adapter, packet, opts) {
    if (!NET_MONITOR_ENABLED || !packet || !Array.isArray(packet.data) || packet.data.length === 0) return;
    const eventName = typeof packet.data[0] === 'string' ? packet.data[0] : String(packet.data[0] || '');
    if (!shouldTrackNetEvent(eventName)) return;

    const recipients = resolveBroadcastRecipients(adapter, opts);
    if (!recipients.size) return;

    const bytesPerMessage = estimateSocketIoPacketBytes(packet);
    const grouped = groupRecipientsByRoom(recipients);
    Object.entries(grouped).forEach(([roomCode, count]) => {
        recordRoomNetworkEvent(roomCode, 'out', eventName, bytesPerMessage, count);
    });
}

function instrumentSocketIoBroadcastMonitor() {
    if (!NET_MONITOR_ENABLED) return;
    const adapter = io.of('/').adapter;
    if (!adapter || adapter.__netMonitorWrapped) return;

    const originalBroadcast = adapter.broadcast.bind(adapter);
    adapter.broadcast = function wrappedBroadcast(packet, opts) {
        try {
            trackBroadcastPacket(adapter, packet, opts || {});
        } catch (_) {}
        return originalBroadcast(packet, opts);
    };

    if (typeof adapter.broadcastWithAck === 'function') {
        const originalBroadcastWithAck = adapter.broadcastWithAck.bind(adapter);
        adapter.broadcastWithAck = function wrappedBroadcastWithAck(packet, opts, clientCountCallback, ack) {
            try {
                trackBroadcastPacket(adapter, packet, opts || {});
            } catch (_) {}
            return originalBroadcastWithAck(packet, opts, clientCountCallback, ack);
        };
    }

    adapter.__netMonitorWrapped = true;
    console.log(`[net] monitor enabled (sample interval: ${NET_MONITOR_LOG_INTERVAL_MS}ms)`);
}

instrumentSocketIoBroadcastMonitor();

function sanitizePersistentId(value) {
    if (typeof value !== 'string') return '';
    return value.trim();
}

function getAdRewardState(persistentId) {
    const pid = sanitizePersistentId(persistentId);
    if (!pid) return null;
    if (!adRewardStateByPersistent[pid]) {
        adRewardStateByPersistent[pid] = {
            instantRespawnPending: false,
            updatedAt: Date.now()
        };
    }
    return adRewardStateByPersistent[pid];
}

function setAdInstantRespawnPending(persistentId, nextPending) {
    const state = getAdRewardState(persistentId);
    if (!state) return null;
    state.instantRespawnPending = !!nextPending;
    state.updatedAt = Date.now();
    return state;
}

function buildAdsStatePayload(persistentId) {
    const state = getAdRewardState(persistentId);
    return {
        instantRespawnPending: !!(state && state.instantRespawnPending)
    };
}

function emitAdsStateToSocket(targetSocket) {
    if (!targetSocket || !targetSocket.authPersistentId) return;
    targetSocket.emit('ads:state', buildAdsStatePayload(targetSocket.authPersistentId));
}

function emitAdsStateToPersistent(persistentId) {
    const pid = sanitizePersistentId(persistentId);
    if (!pid) return;
    const payload = buildAdsStatePayload(pid);
    Array.from(io.sockets.sockets.values()).forEach((s) => {
        if (!s || s.authPersistentId !== pid) return;
        s.emit('ads:state', payload);
    });
}

function finalizeRoomAdRewards(room) {
    if (!room || !room.players) return;
    const touched = new Set();
    Object.values(room.players).forEach((player) => {
        if (!player || !player.persistentId) return;
        const pid = player.persistentId;
        if (player.instantRespawnActiveAtMatchStart) {
            setAdInstantRespawnPending(pid, !player.instantRespawnUsedThisMatch);
        }
        player.instantRespawnActiveAtMatchStart = false;
        player.instantRespawnUsedThisMatch = false;
        player.instantRespawnCharges = 0;
        touched.add(pid);
    });
    touched.forEach((pid) => emitAdsStateToPersistent(pid));
}

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

function issueSessionToken(persistentId, name, extras = {}) {
    const payload = {
        pid: persistentId,
        name: name || 'Player',
        exp: Date.now() + SESSION_TTL_MS,
        nonce: crypto.randomBytes(12).toString('hex')
    };
    if (extras && typeof extras.uid === 'string' && extras.uid) payload.uid = extras.uid;
    if (extras && typeof extras.fc === 'string' && extras.fc) payload.fc = extras.fc;
    if (extras && typeof extras.un === 'string' && extras.un) payload.un = extras.un;
    const payloadB64 = toBase64Url(JSON.stringify(payload));
    const sig = signSessionPayload(payloadB64);
    return `${payloadB64}.${sig}`;
}

function verifySessionTokenAny(token) {
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
    if (!payload) return null;
    if (!payload.exp || Date.now() > payload.exp) return null;
    return payload;
}

function verifySessionToken(token, persistentId) {
    const payload = verifySessionTokenAny(token);
    if (!payload || payload.pid !== persistentId) return null;
    return payload;
}

function extractAuthTokenFromRequest(req) {
    if (req && req.body && typeof req.body.currentProfileToken === 'string' && req.body.currentProfileToken.trim()) {
        return req.body.currentProfileToken.trim();
    }
    const hdr = req && req.headers ? req.headers.authorization : '';
    if (typeof hdr === 'string' && hdr.toLowerCase().startsWith('bearer ')) {
        return hdr.slice(7).trim();
    }
    return '';
}

function extractPersistentIdFromRequest(req) {
    if (req && req.body && typeof req.body.persistentId === 'string') {
        const v = req.body.persistentId.trim();
        if (v) return v;
    }
    return '';
}

function normalizeAuthEmail(raw) {
    return String(raw || '').trim().toLowerCase();
}

function normalizeAuthUsername(raw) {
    return String(raw || '').trim().toLowerCase();
}

function sanitizePlayerNickname(raw) {
    const base = String(raw || '').trim();
    if (!base) return '';
    return base.slice(0, 12);
}

function validateAuthPasswordPolicy(password) {
    const pass = String(password || '');
    if (/[^\x21-\x7E]/.test(pass)) {
        return { ok: false, message: 'Password must use English letters, numbers, and symbols only.' };
    }
    if (!/[A-Z]/.test(pass)) {
        return { ok: false, message: 'Password must include at least one uppercase letter.' };
    }
    if (!/[a-z]/.test(pass)) {
        return { ok: false, message: 'Password must include at least one lowercase letter.' };
    }
    if (!/[0-9]/.test(pass)) {
        return { ok: false, message: 'Password must include at least one number.' };
    }
    if (!/[^A-Za-z0-9]/.test(pass)) {
        return { ok: false, message: 'Password must include at least one symbol.' };
    }
    return { ok: true, message: '' };
}

function scryptAsync(password, salt, opts) {
    return new Promise((resolve, reject) => {
        crypto.scrypt(password, salt, 64, opts, (err, derivedKey) => {
            if (err) reject(err);
            else resolve(derivedKey);
        });
    });
}

async function hashPasswordForAuth(password) {
    const salt = crypto.randomBytes(16).toString('hex');
    const opts = { N: AUTH_SCRYPT_N, r: AUTH_SCRYPT_R, p: AUTH_SCRYPT_P };
    const key = await scryptAsync(password, salt, opts);
    return `scrypt$${AUTH_SCRYPT_N}$${AUTH_SCRYPT_R}$${AUTH_SCRYPT_P}$${salt}$${key.toString('hex')}`;
}

async function verifyPasswordForAuth(password, storedHash) {
    if (typeof storedHash !== 'string' || !storedHash.startsWith('scrypt$')) return false;
    const parts = storedHash.split('$');
    if (parts.length !== 6) return false;
    const n = Number(parts[1]);
    const r = Number(parts[2]);
    const p = Number(parts[3]);
    const salt = parts[4];
    const expectedHex = parts[5];
    if (!Number.isFinite(n) || !Number.isFinite(r) || !Number.isFinite(p) || !salt || !expectedHex) return false;
    const derived = await scryptAsync(password, salt, { N: n, r, p });
    const given = Buffer.from(expectedHex, 'hex');
    if (!given.length || given.length !== derived.length) return false;
    return crypto.timingSafeEqual(derived, given);
}

function resolveAuthDeviceContext(req) {
    const token = extractAuthTokenFromRequest(req);
    const tokenPayload = verifySessionTokenAny(token);
    const bodyPersistentId = extractPersistentIdFromRequest(req);
    const persistentId = bodyPersistentId || (tokenPayload && tokenPayload.pid ? tokenPayload.pid : '');
    const profileId = tokenPayload && typeof tokenPayload.uid === 'string' ? tokenPayload.uid : '';
    return { token, tokenPayload, persistentId, profileId };
}

function issueSessionForProfile(persistentId, profileSnapshot) {
    const safePersistentId = String(persistentId || '').trim();
    const safeProfile = profileSnapshot || {};
    const sessionExp = Date.now() + SESSION_TTL_MS;
    const token = issueSessionToken(safePersistentId, safeProfile.nickname || 'Player', {
        uid: safeProfile.id || '',
        fc: safeProfile.friendCode || '',
        un: safeProfile.username || ''
    });
    sessions[safePersistentId] = {
        token,
        name: safeProfile.nickname || 'Player',
        exp: sessionExp,
        profileId: safeProfile.id || null,
        friendCode: safeProfile.friendCode || null,
        username: safeProfile.username || null,
        isGuest: !!safeProfile.isGuest
    };
    return {
        token,
        exp: sessionExp,
        profileId: safeProfile.id || null,
        friendCode: safeProfile.friendCode || null,
        username: safeProfile.username || null,
        isGuest: !!safeProfile.isGuest
    };
}

function sendAuthError(res, status, code, message, extra = {}) {
    res.status(status).json({
        ok: false,
        error: code,
        message,
        ...extra
    });
}

function ensurePersistentAuthStore(res) {
    if (!AUTH_REQUIRE_PERSISTENT_STORE) return true;
    if (identityStore && identityStore.mode === 'postgres') return true;
    sendAuthError(
        res,
        503,
        'AUTH_PERSISTENCE_NOT_CONFIGURED',
        'Persistent account storage is not configured. Set DATABASE_URL to a PostgreSQL instance.'
    );
    return false;
}

function mapIdentityError(res, err) {
    const code = err && err.code ? String(err.code) : '';
    if (code === 'EMAIL_ALREADY_USED') return sendAuthError(res, 409, code, 'Email already used.');
    if (code === 'USERNAME_TAKEN') return sendAuthError(res, 409, code, 'Username is already taken.');
    if (code === 'PROFILE_ALREADY_LINKED') return sendAuthError(res, 409, code, 'Profile is already linked to an account.');
    if (code === 'INVALID_EMAIL') return sendAuthError(res, 400, code, 'Invalid email.');
    if (code === 'INVALID_USERNAME') return sendAuthError(res, 400, code, 'Invalid username.');
    if (code === 'INVALID_VERIFICATION_CODE') return sendAuthError(res, 400, code, 'Invalid verification code.');
    if (code === 'VERIFICATION_CODE_EXPIRED') return sendAuthError(res, 400, code, 'Verification code expired.');
    if (code === 'VERIFICATION_RATE_LIMITED') {
        const retryAfterMs = err && Number.isFinite(err.retryAfterMs) ? Math.max(0, Math.floor(err.retryAfterMs)) : undefined;
        return sendAuthError(res, 429, code, 'Verification rate limited.', retryAfterMs !== undefined ? { retryAfterMs } : {});
    }
    if (code === 'ACCOUNT_NOT_FOUND') return sendAuthError(res, 401, 'INVALID_CREDENTIALS', 'Invalid credentials.');
    if (code === 'ACCOUNT_ALREADY_ACTIVE') return sendAuthError(res, 409, code, 'Account already active.');
    if (code === 'EMAIL_NOT_VERIFIED') return sendAuthError(res, 403, code, 'Email is not verified.');
    if (code === 'PROFILE_NOT_FOUND') return sendAuthError(res, 404, code, 'Profile not found.');
    if (code === 'INVALID_DEVICE_OR_PROFILE') return sendAuthError(res, 400, code, 'Invalid device or profile.');
    if (code === 'EMAIL_DELIVERY_NOT_CONFIGURED') {
        return sendAuthError(res, 503, code, 'Verification email service is not configured on this server.');
    }
    if (code === 'EMAIL_DELIVERY_FAILED') {
        return sendAuthError(res, 503, code, 'Could not send verification email right now. Try again.');
    }
    console.error('[auth] unhandled identity error', err && err.message ? err.message : err);
    return sendAuthError(res, 500, 'AUTH_INTERNAL_ERROR', 'Authentication service error.');
}

function authFlowError(code, message, extra = {}) {
    const err = new Error(message || 'Authentication flow error.');
    err.code = String(code || 'AUTH_FLOW_ERROR');
    if (extra && typeof extra === 'object') Object.assign(err, extra);
    return err;
}

function createAuthEmailTransport() {
    if (!AUTH_SMTP_HOST || !AUTH_SMTP_USER || !AUTH_SMTP_PASS || !AUTH_EMAIL_FROM) {
        return null;
    }
    try {
        return nodemailer.createTransport({
            host: AUTH_SMTP_HOST,
            port: AUTH_SMTP_PORT,
            secure: AUTH_SMTP_SECURE,
            auth: {
                user: AUTH_SMTP_USER,
                pass: AUTH_SMTP_PASS
            }
        });
    } catch (err) {
        console.error('[auth-email] failed to initialize SMTP transport', err && err.message ? err.message : err);
        return null;
    }
}

const authEmailTransport = createAuthEmailTransport();
const authEmailReady = !!authEmailTransport;
if (!authEmailReady) {
    console.warn('[auth-email] SMTP not configured. Verification will require OTP fallback mode.');
}

function authOtpFallbackPayload(verificationCode, expiresAt) {
    return {
        delivery: 'otp_fallback',
        devVerificationCode: verificationCode,
        devExpiresAt: expiresAt || null
    };
}

async function sendVerificationEmail(params) {
    const email = String(params && params.email ? params.email : '').trim();
    const verificationCode = String(params && params.verificationCode ? params.verificationCode : '').trim();
    const username = String(params && params.username ? params.username : '').trim();
    const expiresAtMs = Number(params && params.expiresAt ? params.expiresAt : 0) || 0;
    if (!email || !verificationCode) {
        throw authFlowError('EMAIL_DELIVERY_FAILED', 'Could not prepare verification email.');
    }
    if (!authEmailReady) {
        throw authFlowError(
            'EMAIL_DELIVERY_NOT_CONFIGURED',
            'Verification email service is not configured on this server.'
        );
    }

    const expiresMinutes = Math.max(
        1,
        Math.ceil(((expiresAtMs || (Date.now() + AUTH_VERIFY_TTL_MS)) - Date.now()) / 60000)
    );
    const minSuffix = expiresMinutes === 1 ? '' : 's';
    const greeting = username ? `Hi ${username},` : 'Hi,';
    const subject = 'HeadShooter verification code';
    const textBody =
        `${greeting}\n\n` +
        `Your HeadShooter verification code is: ${verificationCode}\n` +
        `This code expires in about ${expiresMinutes} minute${minSuffix}.\n\n` +
        'If this was not you, please ignore this message.';
    const htmlBody =
        `<p>${greeting}</p>` +
        `<p>Your HeadShooter verification code is:</p>` +
        `<p style="font-size:24px;font-weight:800;letter-spacing:4px;margin:10px 0;">${verificationCode}</p>` +
        `<p>This code expires in about <strong>${expiresMinutes} minute${minSuffix}</strong>.</p>` +
        '<p>If this was not you, you can ignore this message.</p>';

    try {
        await authEmailTransport.sendMail({
            from: AUTH_EMAIL_FROM,
            to: email,
            subject,
            text: textBody,
            html: htmlBody
        });
        return { delivery: 'email' };
    } catch (err) {
        console.error('[auth-email] send failed', err && err.message ? err.message : err);
        throw authFlowError('EMAIL_DELIVERY_FAILED', 'Could not send verification email right now.');
    }
}

async function resolveVerificationDelivery(params) {
    const verificationCode = String(params && params.verificationCode ? params.verificationCode : '').trim();
    const expiresAt = params && params.expiresAt ? params.expiresAt : null;

    if (authEmailReady) {
        try {
            return await sendVerificationEmail(params);
        } catch (err) {
            if (AUTH_ALLOW_OTP_FALLBACK && verificationCode) {
                return authOtpFallbackPayload(verificationCode, expiresAt);
            }
            throw err;
        }
    }

    if (AUTH_ALLOW_OTP_FALLBACK && verificationCode) {
        return authOtpFallbackPayload(verificationCode, expiresAt);
    }
    throw authFlowError(
        'EMAIL_DELIVERY_NOT_CONFIGURED',
        'Verification email is not available yet. Configure SMTP first.'
    );
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
        targetSocket.emit('party:lobbyState', payload);
        return;
    }
    io.to(roomCode).emit('lobbyUpdate', payload);
    io.to(roomCode).emit('party:lobbyState', payload);
}

function makePartyInviteId() {
    return `pinv_${crypto.randomBytes(8).toString('hex')}`;
}

function socketsByProfileId(profileId) {
    if (!profileId) return [];
    return Array.from(io.sockets.sockets.values()).filter((s) => s && s.profileId === profileId);
}

function notifyInviteStatus(invite, statusOverride = '') {
    if (!invite) return;
    const status = statusOverride || invite.status || 'expired';
    const payload = {
        inviteId: invite.id,
        roomCode: invite.roomCode,
        fromProfileId: invite.fromProfileId || null,
        toProfileId: invite.toProfileId || null,
        targetName: invite.toName || null,
        status
    };

    socketsByProfileId(invite.toProfileId).forEach((s) => {
        if (status === 'expired' || status === 'cancelled') {
            s.emit('party:inviteExpired', payload);
        }
    });
    socketsByProfileId(invite.fromProfileId).forEach((s) => {
        s.emit('party:inviteResponded', payload);
    });
}

function cleanupExpiredPartyInvites(now = Date.now()) {
    const pruneBefore = now - (PARTY_INVITE_TTL_MS * 10);
    Object.values(partyInvitesById).forEach((invite) => {
        if (!invite) return;
        if (invite.status === 'pending' && now >= Number(invite.expiresAt || 0)) {
            invite.status = 'expired';
            invite.respondedAt = now;
            notifyInviteStatus(invite, 'expired');
            return;
        }
        if (invite.status !== 'pending' && Number(invite.respondedAt || 0) > 0 && Number(invite.respondedAt || 0) < pruneBefore) {
            delete partyInvitesById[invite.id];
        }
    });
}

function invalidatePartyInvitesForRoom(roomCode, status = 'cancelled') {
    if (!roomCode) return;
    const now = Date.now();
    Object.values(partyInvitesById).forEach((invite) => {
        if (!invite || invite.status !== 'pending') return;
        if (invite.roomCode !== roomCode) return;
        invite.status = status;
        invite.respondedAt = now;
        notifyInviteStatus(invite, status);
    });
}

function clonePlayersForResults(room) {
    const out = {};
    Object.entries(room.players || {}).forEach(([key, p]) => {
        out[key] = {
            id: p.id,
            profileId: p.profileId || null,
            persistentId: p.persistentId || null,
            friendCode: p.friendCode || null,
            username: p.username || null,
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

const PLAYER_STATE_FIELDS = [
    'name',
    'x',
    'y',
    'angle',
    'hp',
    'maxHp',
    'kills',
    'deaths',
    'killstreak',
    'hasShield',
    'invisible',
    'speedBoost',
    'shieldExpire',
    'invisExpire',
    'speedExpire',
    'charging',
    'lastProcessedInput'
];
const PROJECTILE_STATE_FIELDS = ['ownerId', 'x', 'y', 'vx', 'vy', 'angle', 'color'];
const BUFF_STATE_FIELDS = ['x', 'y', 'type', 'active', 'takenTime'];
const STATE_FLOAT_EPSILON = {
    x: 0.01,
    y: 0.01,
    vx: 0.01,
    vy: 0.01,
    angle: 0.001
};

function createRoomSyncState() {
    return {
        lastSnapshotAt: 0,
        players: Object.create(null),
        projectiles: Object.create(null),
        buffs: Object.create(null)
    };
}

function resetRoomSyncState(room) {
    if (!room) return;
    room.stateSync = createRoomSyncState();
}

function ensureRoomSyncState(room) {
    if (!room) return createRoomSyncState();
    if (!room.stateSync) {
        room.stateSync = createRoomSyncState();
    }
    return room.stateSync;
}

function buildPlayerStatePacket(p) {
    return {
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
    };
}

function buildProjectileStatePacket(proj) {
    const packet = {
        id: proj.id,
        ownerId: proj.ownerId,
        x: proj.x,
        y: proj.y,
        vx: proj.vx,
        vy: proj.vy,
        angle: proj.angle
    };
    if (proj.color) packet.color = proj.color;
    return packet;
}

function buildBuffStatePacket(buff) {
    return {
        id: buff.id,
        x: buff.x,
        y: buff.y,
        type: buff.type,
        active: !!buff.active,
        takenTime: buff.takenTime || 0
    };
}

function toStateMap(list) {
    const map = Object.create(null);
    (list || []).forEach((item) => {
        if (!item || !item.id) return;
        map[item.id] = item;
    });
    return map;
}

function stateValuesEqual(field, a, b) {
    if (a === b) return true;
    if (typeof a === 'number' && typeof b === 'number' && Number.isFinite(a) && Number.isFinite(b)) {
        const eps = STATE_FLOAT_EPSILON[field];
        if (eps !== undefined) return Math.abs(a - b) <= eps;
    }
    return false;
}

function diffStateMaps(currentMap, previousMap, fields) {
    const upserts = [];
    const removed = [];
    const prev = previousMap || Object.create(null);

    Object.keys(currentMap || {}).forEach((id) => {
        const nextEntity = currentMap[id];
        const prevEntity = prev[id];
        if (!prevEntity) {
            upserts.push(nextEntity);
            return;
        }
        const patch = { id };
        fields.forEach((field) => {
            if (!stateValuesEqual(field, nextEntity[field], prevEntity[field])) {
                patch[field] = nextEntity[field];
            }
        });
        if (Object.keys(patch).length > 1) {
            upserts.push(patch);
        }
    });

    Object.keys(prev).forEach((id) => {
        if (!currentMap[id]) removed.push(id);
    });

    return { upserts, removed };
}

function buildRoomStateSnapshot(room) {
    const players = Object.values(room.players).map(buildPlayerStatePacket);
    const projectiles = room.projectiles.map(buildProjectileStatePacket);
    const buffs = room.buffs.map(buildBuffStatePacket);
    return {
        players,
        projectiles,
        buffs,
        playersMap: toStateMap(players),
        projectilesMap: toStateMap(projectiles),
        buffsMap: toStateMap(buffs)
    };
}

function resetRoomForLobby(roomCode) {
    const room = rooms[roomCode];
    if (!room) return;
    resetRoomSyncState(room);
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
        p.instantRespawnCharges = 0;
        p.instantRespawnUsedThisMatch = false;
        p.instantRespawnActiveAtMatchStart = false;
    });
}

function generateRoomCode() {
    const code = Math.floor(10000 + Math.random() * 90000).toString();
    return rooms[code] ? generateRoomCode() : code;
}

function createRoom(leaderId, leaderName, leaderPersistentId, leaderProfileMeta = null) {
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
        stateSync: createRoomSyncState(),
        lastUpdate: Date.now()
    };
    
    // Add leader
    rooms[code].players[leaderId] = makePlayer(leaderId, leaderName, leaderPersistentId, true, leaderProfileMeta);
    
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

function closestPointOnSegment(ax, ay, bx, by, px, py) {
    const abx = bx - ax;
    const aby = by - ay;
    const abLenSq = (abx * abx) + (aby * aby);
    if (abLenSq <= 1e-6) {
        return { x: ax, y: ay, t: 0 };
    }
    let t = (((px - ax) * abx) + ((py - ay) * aby)) / abLenSq;
    if (t < 0) t = 0;
    if (t > 1) t = 1;
    return {
        x: ax + (abx * t),
        y: ay + (aby * t),
        t
    };
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
        const rewardState = getAdRewardState(player.persistentId);
        const hadInstantRespawn = !!(rewardState && rewardState.instantRespawnPending);
        player.x = spawn.x;
        player.y = spawn.y;
        player.hp = 3;
        player.maxHp = 3;
        player.kills = 0;
        player.deaths = 0;
        player.killstreak = 0;
        player.instantRespawnActiveAtMatchStart = hadInstantRespawn;
        player.instantRespawnUsedThisMatch = false;
        player.instantRespawnCharges = hadInstantRespawn ? INSTANT_RESPAWN_MATCH_CHARGES : 0;
        if (hadInstantRespawn && player.persistentId) {
            setAdInstantRespawnPending(player.persistentId, false);
        }
    });
    playerList.forEach((player) => {
        if (player && player.persistentId) emitAdsStateToPersistent(player.persistentId);
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
            beginRoomNetworkMatch(roomCode);
            io.to(roomCode).emit('gameStart', { startTime: room.gameStartTime });
        }
    }, COUNTDOWN_DURATION);
    
    return true;
}


// ==================== GAME LOGIC ====================
function respawnRoomPlayer(roomCode, player) {
    const room = rooms[roomCode];
    if (!room || !player) return false;
    if (player.hp > 0) return false;

    const carrySeq = Number.isFinite(player.inputSeq)
        ? player.inputSeq
        : ((player.input && Number.isFinite(player.input.seq)) ? player.input.seq : 0);
    const spawn = getNextSpawn(roomCode);
    player.x = spawn.x;
    player.y = spawn.y;
    player.maxHp = 3; // Reset Extra Core on death
    player.hp = player.maxHp;
    player.hasShield = false;
    player.shieldExpire = 0;
    player.invisible = false;
    player.invisExpire = 0;
    player.speedBoost = false;
    player.speedExpire = 0;
    player.charging = false;
    player.chargeStartedAt = 0;
    player.lastShotAt = 0;
    player.inputSeq = carrySeq;
    player.input = { w: false, a: false, s: false, d: false, angle: player.angle || 0, charging: false, seq: carrySeq };
    player.inputIntegrity = { lastMask: 0, lastAt: 0, togglePoints: 0, windowStart: 0 };
    player.diedAt = 0;

    io.to(roomCode).emit('playerRespawn', {
        playerId: player.id,
        x: spawn.x,
        y: spawn.y,
        hp: player.hp
    });
    return true;
}

function updatePlayers(roomCode, dt) {
    const room = rooms[roomCode];
    if (!room || room.state !== 'playing') return;

    const now = Date.now();
    const mapKey = room.selectedMap || room.map || 'forest';

    Object.values(room.players).forEach(player => {
        if (player.hp <= 0) {
            if (player.diedAt && (now - player.diedAt) >= PLAYER_RESPAWN_DELAY) {
                respawnRoomPlayer(roomCode, player);
            }
            return;
        }

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

    const validProjectiles = [];
    const mapKey = room.selectedMap || room.map || 'forest';
    
    room.projectiles.forEach(proj => {
        const prevX = proj.x;
        const prevY = proj.y;
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
        let segStartX = prevX;
        let segStartY = prevY;
        let segEndX = proj.x;
        let segEndY = proj.y;
        const segDx = segEndX - segStartX;
        const segDy = segEndY - segStartY;
        const segLen = Math.sqrt((segDx * segDx) + (segDy * segDy));
        if (segLen > 1e-6) {
            const ux = segDx / segLen;
            const uy = segDy / segLen;
            // Use arrow tip for hit tests to match client visuals.
            segStartX += ux * PROJECTILE_TIP_OFFSET;
            segStartY += uy * PROJECTILE_TIP_OFFSET;
            segEndX += ux * PROJECTILE_TIP_OFFSET;
            segEndY += uy * PROJECTILE_TIP_OFFSET;
        }

        let bestHit = null;
        for (const player of Object.values(room.players)) {
            if (player.hp <= 0 || player.id === proj.ownerId) continue;

            const closest = closestPointOnSegment(segStartX, segStartY, segEndX, segEndY, player.x, player.y);
            const dx = closest.x - player.x;
            const dy = closest.y - player.y;
            const distSq = (dx * dx) + (dy * dy);
            if (distSq >= (PROJECTILE_HIT_RADIUS * PROJECTILE_HIT_RADIUS)) continue;

            if (!bestHit || closest.t < bestHit.t || (closest.t === bestHit.t && distSq < bestHit.distSq)) {
                bestHit = { player, closest, distSq, t: closest.t };
            }
        }
        
        if (bestHit) {
            const victim = bestHit.player;
            const dist = Math.sqrt(bestHit.distSq);
            const isHeadshot = dist <= PROJECTILE_HEADSHOT_RADIUS;
            const blockedByShield = !!victim.hasShield;
            const headshot = !blockedByShield && isHeadshot;
            const hitType = blockedByShield ? 'shield' : 'player';

            io.to(roomCode).emit('hitEffect', {
                x: bestHit.closest.x,
                y: bestHit.closest.y,
                type: hitType,
                headshot: headshot,
                victimId: victim.id,
                ownerId: proj.ownerId
            });
            console.log('[srv] hitEffect', roomCode, hitType, 'headshot=', headshot, 'owner=', proj.ownerId, 'victim=', victim.id);

            // Shield blocks exactly one hit, even headshots.
            if (blockedByShield) {
                victim.hasShield = false;
                io.to(roomCode).emit('shieldBreak', { playerId: victim.id });
            } else {
                victim.hp = headshot ? 0 : victim.hp - 1;
                if (victim.hp <= 0) {
                    handleKill(roomCode, proj.ownerId, victim.id, headshot);
                }
            }
        } else {
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
    const now = Date.now();
    
    if (!killer || !victim) return;
    
    // Update stats
    killer.kills++;
    killer.killstreak++;
    victim.deaths++;
    victim.killstreak = 0;
    victim.hp = 0;
    // Death must clear temporary buffs/states to avoid hidden/stuck players after respawn.
    victim.hasShield = false;
    victim.shieldExpire = 0;
    victim.invisible = false;
    victim.invisExpire = 0;
    victim.speedBoost = false;
    victim.speedExpire = 0;
    victim.charging = false;
    victim.chargeStartedAt = 0;
    victim.lastShotAt = 0;
    victim.diedAt = now;
    
    // Kill chain tracking
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

    let usedInstantRespawn = false;
    let remainingInstantRespawnCharges = 0;
    if (victim.instantRespawnCharges > 0) {
        victim.instantRespawnCharges = Math.max(0, victim.instantRespawnCharges - 1);
        victim.instantRespawnUsedThisMatch = true;
        usedInstantRespawn = true;
        remainingInstantRespawnCharges = victim.instantRespawnCharges;
    }
    
    // Schedule respawn
    if (!usedInstantRespawn) {
        setTimeout(() => {
            const roomNow = rooms[roomCode];
            if (!roomNow) return;

            let victimRef = roomNow.players[victimId];
            if (!victimRef && victim.persistentId) {
                const entry = Object.entries(roomNow.players || {}).find(([, p]) => p && p.persistentId === victim.persistentId);
                if (entry) victimRef = entry[1];
            }
            if (!victimRef) return;

            respawnRoomPlayer(roomCode, victimRef);
        }, PLAYER_RESPAWN_DELAY);
    }
    
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

    if (usedInstantRespawn) {
        respawnRoomPlayer(roomCode, victim);
        io.to(roomCode).emit('instantRespawnUsed', {
            playerId: victim.id,
            remainingCharges: remainingInstantRespawnCharges
        });
    }
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

function makePlayer(socketId, playerName, persistentId, isLeader, profileMeta = null) {
    const safeMeta = profileMeta && typeof profileMeta === 'object' ? profileMeta : {};
    return {
        id: socketId,
        profileId: safeMeta.profileId || null,
        persistentId: persistentId || null,
        friendCode: safeMeta.friendCode || null,
        username: safeMeta.username || null,
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
        diedAt: 0,
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
        instantRespawnCharges: 0,
        instantRespawnUsedThisMatch: false,
        instantRespawnActiveAtMatchStart: false,
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

    if (NET_MONITOR_ENABLED) {
        const originalSocketEmit = socket.emit.bind(socket);
        socket.emit = function monitoredSocketEmit(eventName, ...args) {
            const roomCode = socket.roomCode;
            if (roomCode && rooms[roomCode] && shouldTrackNetEvent(eventName)) {
                const bytes = estimateSocketIoEventBytes(eventName, args);
                recordRoomNetworkEvent(roomCode, 'out', eventName, bytes, 1);
            }
            return originalSocketEmit(eventName, ...args);
        };

        socket.onAny((eventName, ...args) => {
            const roomCode = socket.roomCode;
            if (!roomCode || !rooms[roomCode] || !shouldTrackNetEvent(eventName)) return;
            const bytes = estimateSocketIoEventBytes(eventName, args);
            recordRoomNetworkEvent(roomCode, 'in', eventName, bytes, 1);
        });
    }
    
    // Heartbeat
    heartbeatInterval = setInterval(() => {
        socket.emit('heartbeat');
    }, 5000);
    socket.heartbeatInterval = heartbeatInterval;

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
            player.profileId = socket.profileId || player.profileId || null;
            player.friendCode = socket.friendCode || player.friendCode || null;
            player.username = socket.username || player.username || null;
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

    function emitFriendsError(code, fallbackMessage, extra = {}) {
        const c = code ? String(code) : 'FRIENDS_ERROR';
        const messages = {
            PROFILE_NOT_FOUND: 'Profile not found.',
            FRIEND_REQUEST_NOT_ALLOWED: 'Friend request is not allowed.',
            FRIEND_REQUEST_ALREADY_EXISTS: 'Friend request already exists.',
            ALREADY_FRIENDS: 'You are already friends.',
            FRIEND_REQUEST_NOT_FOUND: 'Friend request not found.',
            FRIEND_REQUEST_NOT_PENDING: 'Friend request is no longer pending.'
        };
        socket.emit('friends:error', {
            ok: false,
            error: c,
            message: messages[c] || fallbackMessage || 'Friends action failed.',
            ...extra
        });
    }

    function findSocketsByProfileId(profileId) {
        if (!profileId) return [];
        return Array.from(io.sockets.sockets.values()).filter((s) => s && s.profileId === profileId);
    }

    async function emitFriendsStateToSocket(targetSocket) {
        if (!targetSocket || !targetSocket.profileId) return null;
        try {
            const state = await identityStore.getFriendsState({ profileId: targetSocket.profileId });
            targetSocket.emit('friends:listUpdated', {
                ok: true,
                profileId: state.profileId,
                friends: state.friends || [],
                incoming: state.incoming || [],
                outgoing: state.outgoing || [],
                incomingCount: Number(state.incomingCount || 0)
            });
            return state;
        } catch (err) {
            emitFriendsError(err && err.code ? err.code : '', err && err.message ? err.message : 'Could not load friends state.');
            return null;
        }
    }

    async function emitFriendsStateToProfile(profileId) {
        if (!profileId) return;
        const sockets = findSocketsByProfileId(profileId);
        if (!sockets.length) return;
        try {
            const state = await identityStore.getFriendsState({ profileId });
            sockets.forEach((s) => {
                s.emit('friends:listUpdated', {
                    ok: true,
                    profileId: state.profileId,
                    friends: state.friends || [],
                    incoming: state.incoming || [],
                    outgoing: state.outgoing || [],
                    incomingCount: Number(state.incomingCount || 0)
                });
            });
        } catch (_) {}
    }

    function emitPartyInviteError(code, fallbackMessage, extra = {}) {
        const c = code ? String(code) : 'PARTY_INVITE_ERROR';
        const messages = {
            PROFILE_NOT_FOUND: 'Profile not found.',
            PARTY_INVITE_NOT_ALLOWED: 'Party invite is not allowed.',
            PARTY_NOT_FOUND: 'Party not found.',
            PARTY_NOT_IN_LOBBY: 'Party is not in lobby.',
            PARTY_FULL: 'Party is full.',
            TARGET_NOT_ONLINE: 'Target player is offline.',
            TARGET_ALREADY_IN_PARTY: 'Target player is already in this party.',
            PARTY_INVITE_ALREADY_EXISTS: 'Invite already sent.',
            PARTY_INVITE_NOT_FOUND: 'Invite not found.',
            PARTY_INVITE_EXPIRED: 'Invite expired.',
            ACTIVE_MATCH_LOCK: 'Cannot switch party while in active match.'
        };
        socket.emit('party:inviteError', {
            ok: false,
            error: c,
            message: messages[c] || fallbackMessage || 'Party invite failed.',
            ...extra
        });
    }

    async function canInviteTargetProfile(fromProfileId, toProfileId) {
        if (!fromProfileId || !toProfileId || fromProfileId === toProfileId) return false;
        try {
            const state = await identityStore.getFriendsState({ profileId: fromProfileId });
            const friends = Array.isArray(state && state.friends) ? state.friends : [];
            return friends.some((f) => f && f.profileId === toProfileId);
        } catch (_) {
            return false;
        }
    }

    function joinSocketToLobbyRoomViaInvite(roomCode) {
        const room = rooms[roomCode];
        if (!room) {
            return { ok: false, error: 'PARTY_NOT_FOUND', message: 'Party not found.' };
        }
        if (room.state !== 'lobby') {
            return { ok: false, error: 'PARTY_NOT_IN_LOBBY', message: 'Party is not in lobby.' };
        }

        const existingEntry = socket.authPersistentId ? findPlayerEntryByPersistentId(room, socket.authPersistentId) : null;
        const hasSlot = Object.keys(room.players || {}).length < MAX_PLAYERS_PER_ROOM;
        if (!existingEntry && !hasSlot) {
            return { ok: false, error: 'PARTY_FULL', message: 'Party is full.' };
        }

        if (socket.roomCode && rooms[socket.roomCode] && socket.roomCode !== roomCode) {
            const current = rooms[socket.roomCode];
            if (current && isActiveMatchState(current.state)) {
                return { ok: false, error: 'ACTIVE_MATCH_LOCK', message: 'Leave your active match first.' };
            }
            removePlayerFromRoom();
            socket.roomCode = null;
            socket.playerKey = socket.id;
            currentRoom = null;
        }

        if (existingEntry && existingEntry[1]) {
            const oldKey = existingEntry[0];
            const player = existingEntry[1];
            if (oldKey !== socket.id) {
                delete room.players[oldKey];
            }
            player.id = socket.id;
            player.name = socket.playerName || player.name;
            player.profileId = socket.profileId || player.profileId || null;
            player.friendCode = socket.friendCode || player.friendCode || null;
            player.username = socket.username || player.username || null;
            player.disconnected = false;
            player.lastUpdate = Date.now();
            room.players[socket.id] = player;
            if (room.leader === oldKey) room.leader = socket.id;
        } else if (!room.players[socket.id]) {
            room.players[socket.id] = makePlayer(socket.id, socket.playerName || 'Player', socket.authPersistentId, false, {
                profileId: socket.profileId || null,
                friendCode: socket.friendCode || null,
                username: socket.username || null
            });
        }

        socket.join(roomCode);
        socket.roomCode = roomCode;
        socket.playerKey = socket.id;
        socket.authRoomCode = roomCode;
        currentRoom = roomCode;

        emitLobbyUpdate(roomCode);
        socket.emit('roomJoined', {
            roomCode,
            players: room.players,
            leaderId: room.leader
        });
        return { ok: true, room };
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
        const enforce = ANTI_CHEAT_MODE === 'enforce' && ANTI_CHEAT_PENALTIES_ENABLED;
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
            actionDetails = {
                reason,
                until: enforce ? (st.blockUntil || 0) : virtualUntil,
                windowStrikes: st.windowStrikes,
                mode: enforce ? 'enforce' : 'observe',
                enforced: enforce
            };
        } else if (st.windowStrikes >= STRIKE_SOFT_BLOCK_THRESHOLD) {
            if (st.level !== 'hard') st.level = 'soft';
            virtualUntil = now + SOFT_BLOCK_MS;
            if (enforce) {
                st.blockUntil = Math.max(st.blockUntil || 0, virtualUntil);
            } else {
                antiCheatMetrics.enforcements.wouldSoftBlock = (antiCheatMetrics.enforcements.wouldSoftBlock || 0) + 1;
            }
            action = 'softBlock';
            actionDetails = {
                reason,
                until: enforce ? (st.blockUntil || 0) : virtualUntil,
                windowStrikes: st.windowStrikes,
                mode: enforce ? 'enforce' : 'observe',
                enforced: enforce
            };
        } else if (st.windowStrikes >= STRIKE_WARN_THRESHOLD && !st.warned) {
            st.warned = true;
            action = 'warn';
            actionDetails = { reason, windowStrikes: st.windowStrikes, mode: enforce ? 'enforce' : 'observe', enforced: false };
        }

        if (action) {
            st.lastAction = action;
            st.lastActionAt = now;
            antiCheatMetrics.enforcements[action] = (antiCheatMetrics.enforcements[action] || 0) + 1;
            pushEscalation(player, action, now, actionDetails);
            if (enforce) {
                socket.emit('antiCheatAction', {
                    action,
                    until: actionDetails && actionDetails.until ? actionDetails.until : (st.blockUntil || 0),
                    windowStrikes: st.windowStrikes,
                    mode: actionDetails && actionDetails.mode ? actionDetails.mode : (enforce ? 'enforce' : 'observe'),
                    enforced: !!(actionDetails && actionDetails.enforced)
                });
            }
        }
    }

    function isEnforcementBlocked(player, eventName) {
        if (ANTI_CHEAT_MODE !== 'enforce' || !ANTI_CHEAT_PENALTIES_ENABLED) return false;
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

    function isActiveMatchState(state) {
        return ACTIVE_MATCH_STATES.has(state);
    }

    function findActiveMatchForPersistentId(persistentId) {
        if (!persistentId) return null;
        for (const [roomCode, room] of Object.entries(rooms)) {
            if (!room || !isActiveMatchState(room.state)) continue;
            const entry = findPlayerEntryByPersistentId(room, persistentId);
            if (!entry) continue;
            return {
                roomCode,
                room,
                key: entry[0],
                player: entry[1]
            };
        }
        return null;
    }

    function resetPlayerRealtimeInputState(player) {
        if (!player) return;
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
        if (player.antiCheatState) {
            player.antiCheatState.windowStart = 0;
            player.antiCheatState.windowStrikes = 0;
            player.antiCheatState.level = 'none';
            player.antiCheatState.warned = false;
            player.antiCheatState.blockUntil = 0;
            player.antiCheatState.lastBlockLogAt = 0;
        }
    }

    function reconnectSocketToMatch(match, preferredName) {
        if (!match || !match.room || !match.player) return false;
        const roomCode = match.roomCode;
        const room = match.room;
        const oldKey = match.key;
        const player = match.player;
        if (!rooms[roomCode] || rooms[roomCode] !== room) return false;

        const oldSocket = oldKey && oldKey !== socket.id ? io.sockets.sockets.get(oldKey) : null;
        if (oldSocket && oldSocket !== socket) {
            oldSocket.leave(roomCode);
            oldSocket.roomCode = null;
            oldSocket.playerKey = oldSocket.id;
            oldSocket.disconnect(true);
        }

        if (oldKey && oldKey !== socket.id && room.players[oldKey]) {
            delete room.players[oldKey];
        }

        if (socket.roomCode && socket.roomCode !== roomCode) {
            socket.leave(socket.roomCode);
        }

        player.id = socket.id;
        player.name = preferredName || socket.playerName || player.name;
        player.profileId = socket.profileId || player.profileId || null;
        player.friendCode = socket.friendCode || player.friendCode || null;
        player.username = socket.username || player.username || null;
        player.disconnected = false;
        player.lastUpdate = Date.now();
        resetPlayerRealtimeInputState(player);
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
        return true;
    }

    function removePlayerFromRoom(options = {}) {
        const { preserveInMatch = false } = options;
        const roomCode = socket.roomCode;
        if (!roomCode || !rooms[roomCode]) return;

        const room = rooms[roomCode];
        const key = socket.playerKey || socket.id;
        const player = room.players[key];
        if (!player) return;

        const inMatch = isActiveMatchState(room.state);
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
            if (isActiveMatchState(room.state)) {
                finishRoomNetworkMatch(roomCode, 'room_deleted_empty');
            }
            invalidatePartyInvitesForRoom(roomCode, 'cancelled');
            delete rooms[roomCode];
            delete netMonitor.rooms[roomCode];
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
    socket.on('registerPlayer', async (data) => {
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

        let profileSnapshot = null;
        try {
            profileSnapshot = await identityStore.ensureGuestProfile({
                persistentId: incomingId,
                nickname: incomingName || 'Player'
            });
        } catch (e) {
            console.error('[identity] ensure guest profile failed', e && e.message ? e.message : e);
            socket.emit('authError', { message: 'Identity service unavailable. Please try again.' });
            return;
        }

        socket.persistentId = incomingId;
        socket.profileId = profileSnapshot && profileSnapshot.id ? profileSnapshot.id : null;
        socket.friendCode = profileSnapshot && profileSnapshot.friendCode ? profileSnapshot.friendCode : null;
        socket.username = profileSnapshot && profileSnapshot.username ? profileSnapshot.username : null;
        socket.isGuest = !(profileSnapshot && profileSnapshot.isGuest === false);
        socket.playerName = (profileSnapshot && profileSnapshot.nickname) || incomingName || 'Player';

        if (socket.roomCode && rooms[socket.roomCode]) {
            const room = rooms[socket.roomCode];
            const entry = findPlayerEntryByPersistentId(room, incomingId);
            if (entry && entry[1]) {
                entry[1].profileId = socket.profileId || null;
                entry[1].friendCode = socket.friendCode || null;
                entry[1].username = socket.username || null;
                entry[1].name = socket.playerName || entry[1].name;
            }
        }

        const tokenPayload = verifySessionToken(incomingToken, incomingId);
        if (tokenPayload) {
            socket.authenticated = true;
            socket.authPersistentId = incomingId;
        } else {
            socket.authenticated = true; // first-time bootstrap from this device
            socket.authPersistentId = incomingId;
        }

        const session = issueSessionForProfile(incomingId, {
            id: socket.profileId,
            nickname: socket.playerName,
            friendCode: socket.friendCode,
            username: socket.username,
            isGuest: socket.isGuest
        });
        socket.emit('sessionToken', {
            token: session.token,
            exp: session.exp,
            profileId: session.profileId,
            friendCode: session.friendCode,
            username: session.username || null,
            isGuest: session.isGuest
        });
        console.log(
            `Player recognized: ${socket.playerName} with ID: ${incomingId}` +
            (socket.profileId ? ` (profile ${socket.profileId})` : '')
        );

        await emitFriendsStateToSocket(socket);
        emitAdsStateToSocket(socket);

        // If this device has a finished match pending, show results after reconnect.
        emitPendingMatchResultsForSocket(socket) || emitRecentRoomResultsForSocket(socket);

        // Auto-reconnect to an active match by persistent ID
        if (!socket.authPersistentId) return;
        const activeMatch = findActiveMatchForPersistentId(socket.authPersistentId);
        if (!activeMatch) return;

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

        reconnectSocketToMatch(activeMatch, socket.playerName);
    });

    socket.on('updateName', async (data, ack) => {
        if (!allowEvent('updateName', 10, 10000)) return;
        if (!socket.authPersistentId) {
            if (typeof ack === 'function') {
                ack({ ok: false, error: 'AUTH_REQUIRED', message: 'Please reconnect and try again.' });
            }
            return;
        }

        const requestedName = sanitizePlayerNickname(data && data.newName);
        if (!requestedName) {
            if (typeof ack === 'function') {
                ack({ ok: false, error: 'INVALID_NAME', message: 'Name must be 1-12 characters.' });
            }
            return;
        }

        try {
            const updatedProfile = await identityStore.ensureGuestProfile({
                persistentId: socket.authPersistentId,
                nickname: requestedName
            });
            if (!updatedProfile || !updatedProfile.id) {
                throw new Error('profile update failed');
            }

            socket.playerName = updatedProfile.nickname || requestedName;
            socket.profileId = updatedProfile.id || socket.profileId || null;
            socket.friendCode = updatedProfile.friendCode || socket.friendCode || null;
            socket.username = updatedProfile.username || socket.username || null;
            socket.isGuest = !(updatedProfile.isGuest === false);

            const touchedRooms = new Set();
            Object.entries(rooms).forEach(([roomCode, room]) => {
                if (!room || !room.players) return;
                Object.values(room.players).forEach((player) => {
                    if (!player) return;
                    const sameProfile = socket.profileId && player.profileId && player.profileId === socket.profileId;
                    const samePersistent = socket.authPersistentId && player.persistentId === socket.authPersistentId;
                    if (!sameProfile && !samePersistent) return;
                    player.name = socket.playerName;
                    player.profileId = socket.profileId || player.profileId || null;
                    player.friendCode = socket.friendCode || player.friendCode || null;
                    player.username = socket.username || player.username || null;
                    touchedRooms.add(roomCode);
                });
            });
            touchedRooms.forEach((roomCode) => emitLobbyUpdate(roomCode));

            Object.values(partyInvitesById).forEach((invite) => {
                if (!invite) return;
                if (socket.profileId && invite.fromProfileId === socket.profileId) {
                    invite.fromName = socket.playerName;
                }
                if (socket.profileId && invite.toProfileId === socket.profileId) {
                    invite.toName = socket.playerName;
                }
            });

            const connectedProfileSockets = Array.from(io.sockets.sockets.values()).filter((s) => s && s.profileId);
            await Promise.all(connectedProfileSockets.map((s) => emitFriendsStateToSocket(s)));

            findSocketsByProfileId(socket.profileId).forEach((s) => {
                if (!s) return;
                s.playerName = socket.playerName;
                s.emit('profile:nicknameUpdated', {
                    profileId: socket.profileId,
                    nickname: socket.playerName
                });
            });

            const session = issueSessionForProfile(socket.authPersistentId, updatedProfile);
            socket.emit('sessionToken', {
                token: session.token,
                exp: session.exp,
                profileId: session.profileId,
                friendCode: session.friendCode,
                username: session.username || null,
                isGuest: session.isGuest
            });

            if (typeof ack === 'function') {
                ack({
                    ok: true,
                    profileId: socket.profileId,
                    nickname: socket.playerName
                });
            }
        } catch (err) {
            if (typeof ack === 'function') {
                ack({
                    ok: false,
                    error: 'NAME_UPDATE_FAILED',
                    message: 'Could not update name right now.'
                });
            }
        }
    });

    socket.on('friends:getList', async () => {
        if (!allowEvent('friends:getList', 30, 10000)) return;
        if (!socket.profileId) {
            emitFriendsError('PROFILE_NOT_FOUND', 'Please register first.');
            return;
        }
        await emitFriendsStateToSocket(socket);
    });

    socket.on('friends:search', async (data, ack) => {
        if (!allowEvent('friends:search', 24, 10000)) return;
        if (!socket.profileId) {
            emitFriendsError('PROFILE_NOT_FOUND', 'Please register first.');
            if (typeof ack === 'function') ack({ ok: false, error: 'PROFILE_NOT_FOUND', results: [] });
            return;
        }

        const query = String(data && data.query ? data.query : '').trim();
        const limit = Math.max(1, Math.min(20, Number(data && data.limit) || 10));
        if (!query) {
            const emptyPayload = { ok: true, query: '', results: [] };
            if (typeof ack === 'function') ack(emptyPayload);
            else socket.emit('friends:searchResult', emptyPayload);
            return;
        }

        try {
            const result = await identityStore.searchFriendProfiles({
                profileId: socket.profileId,
                query,
                limit
            });
            const payload = {
                ok: true,
                query,
                results: Array.isArray(result && result.results) ? result.results : []
            };
            if (typeof ack === 'function') ack(payload);
            else socket.emit('friends:searchResult', payload);
        } catch (err) {
            const payload = {
                ok: false,
                error: err && err.code ? err.code : 'FRIENDS_SEARCH_FAILED',
                message: err && err.message ? err.message : 'Search failed.',
                results: []
            };
            if (typeof ack === 'function') ack(payload);
            emitFriendsError(payload.error, payload.message);
        }
    });

    socket.on('friends:sendRequest', async (data, ack) => {
        if (!allowEvent('friends:sendRequest', 12, 10000)) return;
        if (!socket.profileId) {
            const payload = { ok: false, error: 'PROFILE_NOT_FOUND', message: 'Please register first.' };
            if (typeof ack === 'function') ack(payload);
            emitFriendsError(payload.error, payload.message);
            return;
        }

        let toProfileId = String(data && data.targetProfileId ? data.targetProfileId : '').trim();
        if (!toProfileId) {
            toProfileId = String(data && data.profileId ? data.profileId : '').trim();
        }
        if (!toProfileId) {
            const payload = { ok: false, error: 'PROFILE_NOT_FOUND', message: 'Target profile is required.' };
            if (typeof ack === 'function') ack(payload);
            emitFriendsError(payload.error, payload.message);
            return;
        }

        try {
            const created = await identityStore.sendFriendRequest({
                fromProfileId: socket.profileId,
                toProfileId
            });
            await emitFriendsStateToProfile(socket.profileId);
            await emitFriendsStateToProfile(toProfileId);

            const fromSnapshot = await identityStore.getProfileSnapshotById(socket.profileId);
            const incomingPayload = {
                ok: true,
                requestId: created.requestId,
                fromProfileId: socket.profileId,
                toProfileId,
                fromNickname: fromSnapshot && fromSnapshot.nickname ? fromSnapshot.nickname : socket.playerName || 'Player',
                fromUsername: fromSnapshot && fromSnapshot.username ? fromSnapshot.username : null,
                fromFriendCode: fromSnapshot && fromSnapshot.friendCode ? fromSnapshot.friendCode : null
            };
            findSocketsByProfileId(toProfileId).forEach((s) => {
                s.emit('friends:incomingRequest', incomingPayload);
            });

            const okPayload = {
                ok: true,
                requestId: created.requestId,
                targetProfileId: toProfileId
            };
            if (typeof ack === 'function') ack(okPayload);
            else socket.emit('friends:requestSent', okPayload);
        } catch (err) {
            const payload = {
                ok: false,
                error: err && err.code ? err.code : 'FRIEND_REQUEST_FAILED',
                message: err && err.message ? err.message : 'Could not send friend request.'
            };
            if (typeof ack === 'function') ack(payload);
            emitFriendsError(payload.error, payload.message);
        }
    });

    socket.on('friends:respondRequest', async (data, ack) => {
        if (!allowEvent('friends:respondRequest', 20, 10000)) return;
        if (!socket.profileId) {
            const payload = { ok: false, error: 'PROFILE_NOT_FOUND', message: 'Please register first.' };
            if (typeof ack === 'function') ack(payload);
            emitFriendsError(payload.error, payload.message);
            return;
        }

        const requestId = String(data && data.requestId ? data.requestId : '').trim();
        const accept = !!(data && (data.accept === true || data.action === 'accept' || data.action === 'accepted'));
        if (!requestId) {
            const payload = { ok: false, error: 'FRIEND_REQUEST_NOT_FOUND', message: 'Request ID is required.' };
            if (typeof ack === 'function') ack(payload);
            emitFriendsError(payload.error, payload.message);
            return;
        }

        try {
            const updated = await identityStore.respondFriendRequest({
                profileId: socket.profileId,
                requestId,
                accept
            });
            await emitFriendsStateToProfile(socket.profileId);
            if (updated && updated.fromProfileId) {
                await emitFriendsStateToProfile(updated.fromProfileId);
            }

            const payload = {
                ok: true,
                requestId,
                status: updated && updated.status ? updated.status : (accept ? 'accepted' : 'rejected')
            };
            if (typeof ack === 'function') ack(payload);
            else socket.emit('friends:requestResponded', payload);
        } catch (err) {
            const payload = {
                ok: false,
                error: err && err.code ? err.code : 'FRIEND_REQUEST_RESPONSE_FAILED',
                message: err && err.message ? err.message : 'Could not respond to friend request.'
            };
            if (typeof ack === 'function') ack(payload);
            emitFriendsError(payload.error, payload.message);
        }
    });

    socket.on('party:inviteFriend', async (data, ack) => {
        if (!allowEvent('party:inviteFriend', 12, 10000)) return;
        cleanupExpiredPartyInvites();

        if (!socket.profileId || !socket.authPersistentId) {
            const payload = { ok: false, error: 'PROFILE_NOT_FOUND', message: 'Please register first.' };
            if (typeof ack === 'function') ack(payload);
            emitPartyInviteError(payload.error, payload.message);
            return;
        }

        const roomCode = socket.roomCode;
        const room = roomCode ? rooms[roomCode] : null;
        ensureSocketPlayerBinding(room);
        if (!room) {
            const payload = { ok: false, error: 'PARTY_NOT_FOUND', message: 'You are not in a party.' };
            if (typeof ack === 'function') ack(payload);
            emitPartyInviteError(payload.error, payload.message);
            return;
        }
        if (room.state !== 'lobby') {
            const payload = { ok: false, error: 'PARTY_NOT_IN_LOBBY', message: 'Invites are allowed in lobby only.' };
            if (typeof ack === 'function') ack(payload);
            emitPartyInviteError(payload.error, payload.message);
            return;
        }

        const senderPlayer = getPlayer(room);
        if (!senderPlayer) {
            const payload = { ok: false, error: 'PARTY_INVITE_NOT_ALLOWED', message: 'Party invite is not allowed.' };
            if (typeof ack === 'function') ack(payload);
            emitPartyInviteError(payload.error, payload.message);
            return;
        }

        const targetProfileId = String(data && data.targetProfileId ? data.targetProfileId : '').trim();
        if (!targetProfileId || targetProfileId === socket.profileId) {
            const payload = { ok: false, error: 'PARTY_INVITE_NOT_ALLOWED', message: 'Invalid target.' };
            if (typeof ack === 'function') ack(payload);
            emitPartyInviteError(payload.error, payload.message);
            return;
        }

        const alreadyInParty = Object.values(room.players || {}).some((p) => p && p.profileId === targetProfileId);
        if (alreadyInParty) {
            const payload = { ok: false, error: 'TARGET_ALREADY_IN_PARTY', message: 'Target is already in this party.' };
            if (typeof ack === 'function') ack(payload);
            emitPartyInviteError(payload.error, payload.message);
            return;
        }

        const canInvite = await canInviteTargetProfile(socket.profileId, targetProfileId);
        if (!canInvite) {
            const payload = { ok: false, error: 'PARTY_INVITE_NOT_ALLOWED', message: 'You can invite friends only.' };
            if (typeof ack === 'function') ack(payload);
            emitPartyInviteError(payload.error, payload.message);
            return;
        }

        const targetSockets = findSocketsByProfileId(targetProfileId);
        if (!targetSockets.length) {
            const payload = { ok: false, error: 'TARGET_NOT_ONLINE', message: 'Target player is offline.' };
            if (typeof ack === 'function') ack(payload);
            emitPartyInviteError(payload.error, payload.message);
            return;
        }

        const existingInvite = Object.values(partyInvitesById).find((inv) => (
            inv &&
            inv.status === 'pending' &&
            inv.roomCode === roomCode &&
            inv.fromProfileId === socket.profileId &&
            inv.toProfileId === targetProfileId &&
            Number(inv.expiresAt || 0) > Date.now()
        ));
        if (existingInvite) {
            const payload = {
                ok: true,
                inviteId: existingInvite.id,
                roomCode,
                targetProfileId,
                message: 'Invite already pending.'
            };
            if (typeof ack === 'function') ack(payload);
            socket.emit('party:inviteSent', payload);
            return;
        }

        let targetSnapshot = null;
        try {
            targetSnapshot = await identityStore.getProfileSnapshotById(targetProfileId);
        } catch (_) {
            targetSnapshot = null;
        }
        const inviteId = makePartyInviteId();
        const now = Date.now();
        const invite = {
            id: inviteId,
            roomCode,
            fromProfileId: socket.profileId,
            fromName: senderPlayer.name || socket.playerName || 'Player',
            toProfileId: targetProfileId,
            toName: targetSnapshot && targetSnapshot.nickname ? targetSnapshot.nickname : 'Player',
            status: 'pending',
            createdAt: now,
            expiresAt: now + PARTY_INVITE_TTL_MS,
            respondedAt: 0
        };
        partyInvitesById[inviteId] = invite;

        const outgoingPayload = {
            ok: true,
            inviteId,
            roomCode,
            fromProfileId: invite.fromProfileId,
            fromName: invite.fromName,
            toProfileId: invite.toProfileId,
            expiresAt: invite.expiresAt
        };
        targetSockets.forEach((s) => {
            s.emit('party:inviteReceived', outgoingPayload);
        });

        const sentPayload = {
            ok: true,
            inviteId,
            roomCode,
            targetProfileId,
            targetName: invite.toName,
            expiresAt: invite.expiresAt
        };
        if (typeof ack === 'function') ack(sentPayload);
        socket.emit('party:inviteSent', sentPayload);
    });

    socket.on('party:inviteRespond', async (data, ack) => {
        if (!allowEvent('party:inviteRespond', 18, 10000)) return;
        cleanupExpiredPartyInvites();

        if (!socket.profileId || !socket.authPersistentId) {
            const payload = { ok: false, error: 'PROFILE_NOT_FOUND', message: 'Please register first.' };
            if (typeof ack === 'function') ack(payload);
            emitPartyInviteError(payload.error, payload.message);
            return;
        }

        const inviteId = String(data && data.inviteId ? data.inviteId : '').trim();
        const accept = !!(data && data.accept);
        if (!inviteId) {
            const payload = { ok: false, error: 'PARTY_INVITE_NOT_FOUND', message: 'Invite ID is required.' };
            if (typeof ack === 'function') ack(payload);
            emitPartyInviteError(payload.error, payload.message);
            return;
        }

        const invite = partyInvitesById[inviteId];
        if (!invite || invite.status !== 'pending') {
            const payload = { ok: false, error: 'PARTY_INVITE_NOT_FOUND', message: 'Invite not found or already handled.' };
            if (typeof ack === 'function') ack(payload);
            emitPartyInviteError(payload.error, payload.message);
            return;
        }
        if (invite.toProfileId !== socket.profileId) {
            const payload = { ok: false, error: 'PARTY_INVITE_NOT_ALLOWED', message: 'This invite is not for this profile.' };
            if (typeof ack === 'function') ack(payload);
            emitPartyInviteError(payload.error, payload.message);
            return;
        }
        if (Date.now() >= Number(invite.expiresAt || 0)) {
            invite.status = 'expired';
            invite.respondedAt = Date.now();
            notifyInviteStatus(invite, 'expired');
            const payload = { ok: false, error: 'PARTY_INVITE_EXPIRED', message: 'Invite expired.' };
            if (typeof ack === 'function') ack(payload);
            emitPartyInviteError(payload.error, payload.message);
            return;
        }

        const senderSockets = findSocketsByProfileId(invite.fromProfileId);
        let targetSnapshot = null;
        try {
            targetSnapshot = await identityStore.getProfileSnapshotById(socket.profileId);
        } catch (_) {
            targetSnapshot = null;
        }
        const targetName = targetSnapshot && targetSnapshot.nickname ? targetSnapshot.nickname : (socket.playerName || 'Player');

        if (!accept) {
            invite.status = 'rejected';
            invite.respondedAt = Date.now();
            const payload = { ok: true, inviteId, status: 'rejected' };
            if (typeof ack === 'function') ack(payload);
            senderSockets.forEach((s) => {
                s.emit('party:inviteResponded', {
                    ok: true,
                    inviteId,
                    status: 'rejected',
                    roomCode: invite.roomCode,
                    targetProfileId: socket.profileId,
                    targetName
                });
            });
            return;
        }

        const room = rooms[invite.roomCode];
        if (!room || room.state !== 'lobby') {
            invite.status = 'expired';
            invite.respondedAt = Date.now();
            senderSockets.forEach((s) => {
                s.emit('party:inviteResponded', {
                    ok: false,
                    inviteId,
                    status: 'expired',
                    roomCode: invite.roomCode,
                    targetProfileId: socket.profileId,
                    targetName
                });
            });
            const payload = { ok: false, error: 'PARTY_NOT_IN_LOBBY', message: 'Party is no longer available.' };
            if (typeof ack === 'function') ack(payload);
            emitPartyInviteError(payload.error, payload.message);
            return;
        }

        const activeMatch = findActiveMatchForPersistentId(socket.authPersistentId);
        if (activeMatch && activeMatch.roomCode !== invite.roomCode) {
            const payload = { ok: false, error: 'ACTIVE_MATCH_LOCK', message: 'Leave your active match first.' };
            if (typeof ack === 'function') ack(payload);
            emitPartyInviteError(payload.error, payload.message);
            return;
        }

        const joinResult = joinSocketToLobbyRoomViaInvite(invite.roomCode);
        if (!joinResult.ok) {
            invite.status = 'expired';
            invite.respondedAt = Date.now();
            senderSockets.forEach((s) => {
                s.emit('party:inviteResponded', {
                    ok: false,
                    inviteId,
                    status: 'expired',
                    roomCode: invite.roomCode,
                    targetProfileId: socket.profileId,
                    targetName
                });
            });
            const payload = {
                ok: false,
                error: joinResult.error || 'PARTY_NOT_IN_LOBBY',
                message: joinResult.message || 'Could not join party.'
            };
            if (typeof ack === 'function') ack(payload);
            emitPartyInviteError(payload.error, payload.message);
            return;
        }

        invite.status = 'accepted';
        invite.respondedAt = Date.now();

        const okPayload = {
            ok: true,
            inviteId,
            status: 'accepted',
            roomCode: invite.roomCode
        };
        if (typeof ack === 'function') ack(okPayload);

        senderSockets.forEach((s) => {
            s.emit('party:inviteResponded', {
                ok: true,
                inviteId,
                status: 'accepted',
                roomCode: invite.roomCode,
                targetProfileId: socket.profileId,
                targetName
            });
        });
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

    socket.on('ads:getState', (ack) => {
        if (!socket.authPersistentId) {
            if (typeof ack === 'function') {
                ack({ ok: false, error: 'AUTH_REQUIRED', message: 'Please reconnect and try again.' });
            }
            return;
        }
        const payload = buildAdsStatePayload(socket.authPersistentId);
        if (typeof ack === 'function') {
            ack(Object.assign({ ok: true }, payload));
            return;
        }
        socket.emit('ads:state', payload);
    });

    socket.on('ads:rewardedCompleted', (data, ack) => {
        if (!allowEvent('ads:rewardedCompleted', 8, 60000)) {
            if (typeof ack === 'function') {
                ack({ ok: false, error: 'RATE_LIMITED', message: 'Too many rewarded requests. Please wait.' });
            }
            return;
        }
        if (!socket.authPersistentId) {
            if (typeof ack === 'function') {
                ack({ ok: false, error: 'AUTH_REQUIRED', message: 'Please reconnect and try again.' });
            }
            return;
        }
        const rewardType = data && typeof data.type === 'string' ? data.type.trim() : '';
        if (rewardType !== 'instant_respawn') {
            if (typeof ack === 'function') {
                ack({ ok: false, error: 'INVALID_REWARD_TYPE', message: 'Unsupported reward type.' });
            }
            return;
        }

        const activeMatch = findActiveMatchForPersistentId(socket.authPersistentId);
        if (activeMatch) {
            if (typeof ack === 'function') {
                ack({ ok: false, error: 'IN_MATCH', message: 'Rewarded ad cannot be claimed during a match.' });
            }
            return;
        }

        if (socket.roomCode && rooms[socket.roomCode]) {
            const room = rooms[socket.roomCode];
            if (room.state === 'lobby') {
                ensureSocketPlayerBinding(room);
                const me = getPlayer(room);
                if (me) {
                    const isLeader = room.leader === (socket.playerKey || socket.id);
                    if (isLeader || me.ready) {
                        if (typeof ack === 'function') {
                            ack({
                                ok: false,
                                error: 'NOT_ALLOWED_WHILE_READY',
                                message: 'Set Not Ready before watching rewarded ads.'
                            });
                        }
                        return;
                    }
                }
            }
        }

        const state = getAdRewardState(socket.authPersistentId);
        if (state && state.instantRespawnPending) {
            if (typeof ack === 'function') {
                ack({ ok: true, instantRespawnPending: true, alreadyActive: true });
            }
            emitAdsStateToPersistent(socket.authPersistentId);
            return;
        }

        setAdInstantRespawnPending(socket.authPersistentId, true);
        if (typeof ack === 'function') {
            ack({ ok: true, instantRespawnPending: true });
        }
        emitAdsStateToPersistent(socket.authPersistentId);
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
        const activeMatch = findActiveMatchForPersistentId(persistentId);
        if (activeMatch) {
            if (reconnectSocketToMatch(activeMatch, (data && data.playerName) || socket.playerName)) return;
            socket.emit('error', { message: 'You already have an active match. Reconnect to continue.' });
            return;
        }
        const displayName = socket.playerName || (data && data.playerName) || 'Soldier';
        const code = createRoom(socket.id, displayName, persistentId, {
            profileId: socket.profileId || null,
            friendCode: socket.friendCode || null,
            username: socket.username || null
        });
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
        console.log(`Room ${code} created by: ${displayName}`);

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
        const persistentId = socket.authPersistentId;
        const activeMatch = findActiveMatchForPersistentId(persistentId);
        if (activeMatch) {
            if (reconnectSocketToMatch(activeMatch, (data && data.playerName) || socket.playerName)) return;
            socket.emit('joinError', { message: 'You already have an active match. Reconnect to continue.' });
            return;
        }
        const code = data.roomCode;
        const room = rooms[code];
        
        if (!room) {
            socket.emit('joinError', { message: 'Room not found' });
            return;
        }

        if (room.state !== 'lobby') {
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
        
        const displayName = socket.playerName || (data && data.playerName) || 'Player';
        room.players[socket.id] = makePlayer(socket.id, displayName, persistentId, false, {
            profileId: socket.profileId || null,
            friendCode: socket.friendCode || null,
            username: socket.username || null
        });
        
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
        invalidatePartyInvitesForRoom(roomCode, 'cancelled');

        room.projectiles = [];
        room.killChains = {};
        room.buffs = [];
        room.nextSpawnIndex = 0;
        resetRoomSyncState(room);
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
            player.diedAt = 0;
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
                beginRoomNetworkMatch(roomCode);
                
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
    
    if (heartbeatInterval) clearInterval(heartbeatInterval);
    if (socket.heartbeatInterval && socket.heartbeatInterval !== heartbeatInterval) {
        clearInterval(socket.heartbeatInterval);
    }
    heartbeatInterval = null;
    socket.heartbeatInterval = null;
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

            const now = Date.now();
            const remainingMs = Math.max(0, (GAME_DURATION * 1000) - (now - room.gameStartTime));
            const stateSync = ensureRoomSyncState(room);
            const snapshot = buildRoomStateSnapshot(room);
            const shouldSendFullSnapshot =
                !stateSync.lastSnapshotAt ||
                (now - stateSync.lastSnapshotAt) >= STATE_FULL_SNAPSHOT_INTERVAL_MS;

            if (shouldSendFullSnapshot) {
                io.to(roomCode).emit('stateUpdate', {
                    mode: 'snapshot',
                    serverTime: now,
                    remainingMs,
                    players: snapshot.players,
                    projectiles: snapshot.projectiles,
                    buffs: snapshot.buffs
                });
                stateSync.lastSnapshotAt = now;
            } else {
                io.to(roomCode).emit('stateUpdate', {
                    mode: 'delta',
                    serverTime: now,
                    remainingMs,
                    playersDelta: diffStateMaps(snapshot.playersMap, stateSync.players, PLAYER_STATE_FIELDS),
                    projectilesDelta: diffStateMaps(snapshot.projectilesMap, stateSync.projectiles, PROJECTILE_STATE_FIELDS),
                    buffsDelta: diffStateMaps(snapshot.buffsMap, stateSync.buffs, BUFF_STATE_FIELDS)
                });
            }
            stateSync.players = snapshot.playersMap;
            stateSync.projectiles = snapshot.projectilesMap;
            stateSync.buffs = snapshot.buffsMap;
             
            // Check game time
            const elapsed = now - room.gameStartTime;
            if (elapsed >= GAME_DURATION * 1000) {
                const resultPlayers = clonePlayersForResults(room);
                const endedAt = now;
                storePendingMatchResults(roomCode, resultPlayers);
                room.lastMatchResults = resultPlayers;
                room.lastMatchEndedAt = endedAt;
                finalizeRoomAdRewards(room);
                resetRoomForLobby(roomCode);
                io.to(roomCode).emit('gameEnd', {
                    roomCode: roomCode,
                    players: resultPlayers,
                    endedAt
                });
                finishRoomNetworkMatch(roomCode, 'timer_elapsed');
                emitLobbyUpdate(roomCode);
            }
        }
    });
}, TICK_MS); // authoritative simulation tick rate

if (NET_MONITOR_ENABLED) {
    setInterval(() => {
        Object.keys(netMonitor.rooms).forEach((roomCode) => {
            logRoomNetworkSample(roomCode);
        });
    }, NET_MONITOR_LOG_INTERVAL_MS);
}

setInterval(() => {
    cleanupExpiredPartyInvites();
}, 2000);

// ==================== AUTH HTTP API ====================
app.post('/auth/signup-link', async (req, res) => {
    try {
        if (!ensurePersistentAuthStore(res)) return;
        const body = req.body || {};
        const emailInput = String(body.email || '').trim();
        const usernameInput = String(body.username || '').trim();
        const password = typeof body.password === 'string' ? body.password : '';
        const emailNorm = normalizeAuthEmail(emailInput);
        if (!AUTH_EMAIL_RE.test(emailNorm)) {
            sendAuthError(res, 400, 'INVALID_EMAIL', 'Invalid email.');
            return;
        }
        if (!AUTH_USERNAME_RE.test(usernameInput)) {
            sendAuthError(res, 400, 'INVALID_USERNAME', 'Username must be 3-24 English letters, numbers, or underscore.');
            return;
        }
        if (password.length < AUTH_PASSWORD_MIN_LEN || password.length > AUTH_PASSWORD_MAX_LEN) {
            sendAuthError(
                res,
                400,
                'WEAK_PASSWORD',
                `Password must be ${AUTH_PASSWORD_MIN_LEN}-${AUTH_PASSWORD_MAX_LEN} characters.`
            );
            return;
        }
        const passwordPolicy = validateAuthPasswordPolicy(password);
        if (!passwordPolicy.ok) {
            sendAuthError(res, 400, 'WEAK_PASSWORD', passwordPolicy.message || 'Password does not meet security requirements.');
            return;
        }

        const authCtx = resolveAuthDeviceContext(req);
        if (!authCtx.persistentId) {
            sendAuthError(res, 401, 'AUTH_CONTEXT_REQUIRED', 'Missing auth context. Provide currentProfileToken or persistentId.');
            return;
        }

        let profileId = authCtx.profileId;
        if (!profileId) {
            const ensured = await identityStore.ensureGuestProfile({
                persistentId: authCtx.persistentId,
                nickname: (authCtx.tokenPayload && authCtx.tokenPayload.name) || 'Player'
            });
            profileId = ensured && ensured.id ? ensured.id : '';
        }
        if (!profileId) {
            sendAuthError(res, 500, 'PROFILE_RESOLUTION_FAILED', 'Could not resolve active profile.');
            return;
        }
        if (!authEmailReady && !AUTH_ALLOW_OTP_FALLBACK) {
            sendAuthError(res, 503, 'EMAIL_DELIVERY_NOT_CONFIGURED', 'Verification email service is not configured on this server.');
            return;
        }

        const passwordHash = await hashPasswordForAuth(password);
        const created = await identityStore.createPendingLinkedAccount({
            profileId,
            email: emailInput,
            username: usernameInput,
            passwordHash,
            codeTtlMs: AUTH_VERIFY_TTL_MS
        });

        const delivery = await resolveVerificationDelivery({
            email: emailInput,
            username: usernameInput,
            verificationCode: created && created.verificationCode ? created.verificationCode : '',
            expiresAt: created && created.expiresAt ? created.expiresAt : null
        });
        const payload = {
            ok: true,
            verification_required: true,
            delivery: delivery && delivery.delivery ? delivery.delivery : 'email',
            message: (delivery && delivery.delivery === 'otp_fallback')
                ? 'Verification required. Use the code shown now (email delivery unavailable).'
                : 'Verification required. Check your email for the code.'
        };
        if (delivery && delivery.devVerificationCode) {
            payload.devVerificationCode = delivery.devVerificationCode;
            payload.devExpiresAt = delivery.devExpiresAt || null;
        }
        res.json(payload);
    } catch (err) {
        mapIdentityError(res, err);
    }
});

app.post('/auth/resend-verification', async (req, res) => {
    try {
        if (!ensurePersistentAuthStore(res)) return;
        const body = req.body || {};
        const emailInput = String(body.email || '').trim();
        const emailNorm = normalizeAuthEmail(emailInput);
        if (!AUTH_EMAIL_RE.test(emailNorm)) {
            sendAuthError(res, 400, 'INVALID_EMAIL', 'Invalid email.');
            return;
        }
        if (!authEmailReady && !AUTH_ALLOW_OTP_FALLBACK) {
            sendAuthError(res, 503, 'EMAIL_DELIVERY_NOT_CONFIGURED', 'Verification email service is not configured on this server.');
            return;
        }

        const resent = await identityStore.resendVerification({
            email: emailInput,
            codeTtlMs: AUTH_VERIFY_TTL_MS,
            resendCooldownMs: AUTH_VERIFY_RESEND_COOLDOWN_MS,
            maxSendsPerHour: AUTH_VERIFY_MAX_SENDS_PER_HOUR
        });

        const delivery = await resolveVerificationDelivery({
            email: emailInput,
            verificationCode: resent && resent.verificationCode ? resent.verificationCode : '',
            expiresAt: resent && resent.expiresAt ? resent.expiresAt : null
        });
        const payload = {
            ok: true,
            resent: true,
            delivery: delivery && delivery.delivery ? delivery.delivery : 'email',
            message: (delivery && delivery.delivery === 'otp_fallback')
                ? 'Verification code resent. Use the code shown now (email delivery unavailable).'
                : 'Verification code resent. Check your email.'
        };
        if (delivery && delivery.devVerificationCode) {
            payload.devVerificationCode = delivery.devVerificationCode;
            payload.devExpiresAt = delivery.devExpiresAt || null;
        }
        res.json(payload);
    } catch (err) {
        if (err && err.code === 'ACCOUNT_NOT_FOUND') {
            res.json({ ok: true, resent: true });
            return;
        }
        mapIdentityError(res, err);
    }
});

app.post('/auth/verify-email', async (req, res) => {
    try {
        if (!ensurePersistentAuthStore(res)) return;
        const body = req.body || {};
        const emailInput = String(body.email || '').trim();
        const otp = String(body.otp || '').trim();
        const emailNorm = normalizeAuthEmail(emailInput);
        if (!AUTH_EMAIL_RE.test(emailNorm)) {
            sendAuthError(res, 400, 'INVALID_EMAIL', 'Invalid email.');
            return;
        }
        if (!otp) {
            sendAuthError(res, 400, 'INVALID_VERIFICATION_CODE', 'Verification code is required.');
            return;
        }

        const verified = await identityStore.verifyEmailCode({
            email: emailInput,
            otp,
            maxVerifyAttempts: AUTH_VERIFY_MAX_ATTEMPTS
        });

        const authCtx = resolveAuthDeviceContext(req);
        let profileSnapshot = null;
        if (verified && verified.profileId) {
            if (authCtx.persistentId) {
                await identityStore.setActiveProfileForDevice({
                    persistentId: authCtx.persistentId,
                    profileId: verified.profileId
                });
            }
            profileSnapshot = await identityStore.getProfileSnapshotById(verified.profileId);
        }

        let session = null;
        if (authCtx.persistentId && profileSnapshot) {
            session = issueSessionForProfile(authCtx.persistentId, profileSnapshot);
        }

        res.json({
            ok: true,
            account_activated: true,
            profile: profileSnapshot,
            session
        });
    } catch (err) {
        mapIdentityError(res, err);
    }
});

app.post('/auth/signin', async (req, res) => {
    try {
        if (!ensurePersistentAuthStore(res)) return;
        const body = req.body || {};
        const login = String(body.email_or_username || '').trim();
        const password = typeof body.password === 'string' ? body.password : '';
        if (!login || !password) {
            sendAuthError(res, 400, 'INVALID_CREDENTIALS', 'Email/username and password are required.');
            return;
        }

        const authCtx = resolveAuthDeviceContext(req);
        if (!authCtx.persistentId) {
            sendAuthError(res, 401, 'AUTH_CONTEXT_REQUIRED', 'Missing auth context. Provide currentProfileToken or persistentId.');
            return;
        }

        const account = await identityStore.findAccountByLogin({ login });
        if (!account) {
            sendAuthError(res, 401, 'INVALID_CREDENTIALS', 'Invalid credentials.');
            return;
        }
        if (account.status === 'pending_verification') {
            sendAuthError(res, 403, 'EMAIL_NOT_VERIFIED', 'Email is not verified.');
            return;
        }
        if (account.status === 'suspended') {
            sendAuthError(res, 403, 'ACCOUNT_SUSPENDED', 'Account is suspended.');
            return;
        }

        const passOk = await verifyPasswordForAuth(password, account.passwordHash);
        if (!passOk) {
            sendAuthError(res, 401, 'INVALID_CREDENTIALS', 'Invalid credentials.');
            return;
        }
        if (!account.profileId) {
            sendAuthError(res, 500, 'ACCOUNT_LINK_CORRUPT', 'Account profile link is missing.');
            return;
        }

        await identityStore.setActiveProfileForDevice({
            persistentId: authCtx.persistentId,
            profileId: account.profileId
        });
        const profileSnapshot = await identityStore.getProfileSnapshotById(account.profileId);
        if (!profileSnapshot) {
            sendAuthError(res, 500, 'PROFILE_RESOLUTION_FAILED', 'Could not resolve account profile.');
            return;
        }
        const session = issueSessionForProfile(authCtx.persistentId, profileSnapshot);
        res.json({
            ok: true,
            profile: profileSnapshot,
            session
        });
    } catch (err) {
        mapIdentityError(res, err);
    }
});

app.post('/auth/logout', async (req, res) => {
    try {
        const body = req.body || {};
        const authCtx = resolveAuthDeviceContext(req);
        if (!authCtx.persistentId) {
            res.json({ ok: true, loggedOut: true });
            return;
        }

        const guestSnapshot = await identityStore.switchToGuestProfileForDevice({
            persistentId: authCtx.persistentId,
            nickname: String(body.guestNickname || (authCtx.tokenPayload && authCtx.tokenPayload.name) || 'Player')
        });
        const session = issueSessionForProfile(authCtx.persistentId, guestSnapshot);
        res.json({
            ok: true,
            loggedOut: true,
            profile: guestSnapshot,
            session
        });
    } catch (err) {
        mapIdentityError(res, err);
    }
});

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

function logServerBanner() {
    console.log(`
╔═══════════════════════════════════════╗
║   HEADSHOOTER SERVER RUNNING          ║
║   Port: ${PORT}                       ║
║   Ready for connections!              ║
╚═══════════════════════════════════════╝
    `);
}

async function startServer() {
    try {
        await identityStore.init();
        const identityMeta = identityStore.describe();
        console.log(`[identity] store mode: ${identityMeta.mode}`);
    } catch (e) {
        console.error('[identity] failed to initialize store', e && e.message ? e.message : e);
        process.exit(1);
        return;
    }

    server.listen(PORT, () => {
        logServerBanner();
    });
}

let isShuttingDown = false;
function handleShutdown(signal) {
    if (isShuttingDown) return;
    isShuttingDown = true;
    console.log(`\nShutting down server (${signal})...`);
    server.close(async () => {
        try {
            await identityStore.close();
        } catch (e) {
            console.warn('[identity] close failed', e && e.message ? e.message : e);
        }
        console.log('Server closed');
        process.exit(0);
    });
}

process.on('SIGINT', () => handleShutdown('SIGINT'));
process.on('SIGTERM', () => handleShutdown('SIGTERM'));

startServer();
