// ==================== HEADSHOOTER SERVER ====================
// Multiplayer server for HeadShooter v15
// Handles rooms, authoritative game logic, and real-time synchronization

const express = require('express');
const http = require('http');
const socketIO = require('socket.io');
const path = require('path');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const io = socketIO(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

const PORT = process.env.PORT || 3000;

// ==================== GAME CONSTANTS ====================
const MAP_WIDTH = 3000;
const MAP_HEIGHT = 2000;
const TICK_RATE = 30;
const TICK_MS = 1000 / TICK_RATE;
const BASE_SPEED_PER_SEC = 127.05; // 2.1175 * 60fps
const MAX_PLAYERS_PER_ROOM = 6;
const BOT_COUNT = 5;
const GAME_DURATION = 120; // 2 minutes
const PLAYER_RESPAWN_DELAY = 3000;
const BUFF_RESPAWN_DELAY = 6000;
const KILL_CHAIN_WINDOW = 6000;
const COUNTDOWN_DURATION = 3000;
const MIN_SHOT_INTERVAL_MS = 140; // server-side fire rate cap
const MAX_ACTIVE_PROJECTILES_PER_PLAYER = 8;
const MAX_INPUT_AHEAD_SEQ = 200;
const SESSION_TTL_MS = 1000 * 60 * 60 * 24 * 14; // 14 days
const SESSION_SECRET = process.env.SESSION_SECRET || 'headshooter-dev-secret-change-me';

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

function resetRoomForLobby(roomCode) {
    const room = rooms[roomCode];
    if (!room) return;
    room.state = 'lobby';
    room.gameStartTime = null;
    room.projectiles = [];
    room.killChains = {};
    room.nextSpawnIndex = 0;
    Object.entries(room.players).forEach(([key, p]) => {
        p.ready = (key === room.leader);
        p.charging = false;
        p.lastShotAt = 0;
        p.inputSeq = 0;
        p.input = {
            w: false,
            a: false,
            s: false,
            d: false,
            angle: p.angle || 0,
            charging: false,
            seq: 0
        };
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
        lastShotAt: 0,
        inputSeq: 0,
        antiCheatStrikes: 0,
        input: { w: false, a: false, s: false, d: false, angle: 0, charging: false, seq: 0 },
        lastUpdate: Date.now()
    };
}

function findPlayerEntryByPersistentId(room, persistentId) {
    if (!room || !persistentId) return null;
    const entry = Object.entries(room.players).find(([, p]) => p && p.persistentId === persistentId);
    return entry || null;
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
    
    // Heartbeat
    heartbeatInterval = setInterval(() => {
        socket.emit('heartbeat');
    }, 5000);

    function getPlayer(room) {
        if (!room) return null;
        const key = socket.playerKey || socket.id;
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
        return rl.count <= maxCount;
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
        if (!socket.playerKey || !room.players[socket.playerKey]) {
            const keyBySocket = room.players[socket.id] ? socket.id : null;
            if (keyBySocket) {
                socket.playerKey = keyBySocket;
            } else if (socket.authPersistentId) {
                const entry = findPlayerEntryByPersistentId(room, socket.authPersistentId);
                if (entry) socket.playerKey = entry[0];
            }
        }
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
        const incomingId = (data && typeof data.id === 'string') ? data.id.trim() : '';
        const incomingName = (data && typeof data.name === 'string') ? data.name.trim() : 'Player';
        const incomingToken = data && typeof data.token === 'string' ? data.token : '';
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

        // Auto-reconnect to an active match by persistent ID
        if (!socket.authPersistentId) return;
        for (const [roomCode, room] of Object.entries(rooms)) {
            if (!room || room.state !== 'playing') continue;
            const entry = findPlayerEntryByPersistentId(room, socket.authPersistentId);
            if (!entry) continue;

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
    
    // Create room
    socket.on('createRoom', (data) => {
    if (!allowEvent('createRoom', 4, 10000)) return;
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
        if (!socket.authenticated || !socket.authPersistentId) {
            socket.emit('authError', { message: 'Please re-authenticate.' });
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
        // استخدم socket.roomCode بدلاً من البحث
        const roomCode = socket.roomCode;
        const room = rooms[roomCode];
        
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
    // Start game (leader only)
    // هذا داخل ملف server.js
    // استبدل السطر 482-513 بهذا الكود:
    socket.on('startGame', () => {
        if (!allowEvent('startGame', 8, 10000)) return;
        const roomCode = socket.roomCode;
        const room = rooms[roomCode];
        
        if (!room) {
            socket.emit('error', { message: 'Room not found' });
            return;
        }
        
        // التأكد أن الشخص هو القائد
        if (room.leader !== socket.id) {
            socket.emit('error', { message: 'Only the leader can start' });
            return;
        }
        
        const playerList = Object.values(room.players);
        
        // فحص جاهزية جميع اللاعبين (ما عدا القائد)
        const allReady = playerList.every(p => {
            if (p.id === room.leader) return true; // القائد دائماً جاهز
            return p.ready === true;
        });
        
        if (!allReady) {
            socket.emit('error', { message: 'Wait! All players must be READY.' });
            return;
        }
        
        // اختيار خريطة عشوائية
        const maps = ['forest', 'canyon', 'island'];
        const selectedMap = maps[Math.floor(Math.random() * maps.length)];
        
        // تحديث حالة الغرفة
        room.state = 'starting';
        room.selectedMap = selectedMap;

        // تهيئة حالة اللعبة
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
            player.lastShotAt = 0;
            player.inputSeq = 0;
            player.antiCheatStrikes = 0;
            player.input = { w:false,a:false,s:false,d:false,angle:0,charging:false,seq:0 };
        });
        
        console.log(`Game starting in room ${roomCode} with map: ${selectedMap}`);
        
        // إرسال أمر البداية للجميع
        io.to(roomCode).emit('gameStarting', {
            mapKey: selectedMap,
            players: room.players,
            roomCode: roomCode
        });
        
        // بعد 3 ثواني (countdown) نبدأ اللعبة فعلياً
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
        if (!allowEvent('playerInput', 120, 1000)) return;
        const roomCode = socket.roomCode;
        const room = rooms[roomCode];
        if (!room || room.state !== 'playing') return;
        const player = getPlayer(room);
        if (!player || player.hp <= 0) return;

        const rawAngle = (data && typeof data.angle === 'number') ? data.angle : player.angle;
        const angle = Number.isFinite(rawAngle) ? rawAngle : player.angle;
        const seq = (data && typeof data.seq === 'number') ? Math.floor(data.seq) : player.inputSeq;
        if (seq < player.inputSeq - 2 || seq > player.inputSeq + MAX_INPUT_AHEAD_SEQ) {
            player.antiCheatStrikes = (player.antiCheatStrikes || 0) + 1;
            return;
        }
        player.inputSeq = Math.max(player.inputSeq || 0, seq);

        player.input = {
            w: !!data.w,
            a: !!data.a,
            s: !!data.s,
            d: !!data.d,
            angle: angle,
            charging: !!data.charging,
            seq: player.inputSeq
        };
        player.lastUpdate = Date.now();

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
        if (!allowEvent('fireProjectile', 35, 1000)) return;
        if (!currentRoom || !rooms[currentRoom] || rooms[currentRoom].state !== 'playing') return;
        
        const player = getPlayer(rooms[currentRoom]);
        if (!player || player.hp <= 0) return;

        const now = Date.now();
        if (player.lastShotAt && (now - player.lastShotAt) < MIN_SHOT_INTERVAL_MS) {
            player.antiCheatStrikes = (player.antiCheatStrikes || 0) + 1;
            return;
        }

        const room = rooms[currentRoom];
        const activeOwnerProjectiles = room.projectiles.filter(p => p.ownerId === player.id).length;
        if (activeOwnerProjectiles >= MAX_ACTIVE_PROJECTILES_PER_PLAYER) {
            player.antiCheatStrikes = (player.antiCheatStrikes || 0) + 1;
            return;
        }

        if (player.invisible) {
            player.invisible = false;
            player.invisExpire = 0;
        }
        
        const rawAngle = typeof data.angle === 'number' ? data.angle : player.angle;
        const angle = Number.isFinite(rawAngle) ? rawAngle : player.angle;
        if (!Number.isFinite(angle)) return;
        const speed = 871.2; // 14.52 * 60fps
        const offset = 25;
        const projectile = {
            id: `proj_${socket.id}_${Date.now()}`,
            ownerId: player.id,
            x: player.x + Math.cos(angle) * offset,
            y: player.y + Math.sin(angle) * offset,
            vx: Math.cos(angle) * speed,
            vy: Math.sin(angle) * speed,
            angle: angle,
            life: 0
        };
        
        player.lastShotAt = now;
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
        const { roomCode, room } = resolved;
        socket.emit('lobbySnapshot', {
            roomCode: room.code,
            leaderId: room.leader,
            players: room.players,
            state: room.state
        });
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
                resetRoomForLobby(roomCode);
                io.to(roomCode).emit('gameEnd', {
                    players: room.players
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

// ==================== START SERVER ====================
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

