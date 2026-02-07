// ==================== HEADSHOOTER SERVER ====================
// Multiplayer server for HeadShooter v15
// Handles rooms, authoritative game logic, and real-time synchronization

const express = require('express');
const http = require('http');
const socketIO = require('socket.io');
const path = require('path');

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
function emitLobbyUpdate(roomCode) {
    const room = rooms[roomCode];
    if (!room) return;
    io.to(roomCode).emit('lobbyUpdate', {
        roomCode: room.code,
        leaderId: room.leader,
        players: room.players,
        state: room.state
    });
}

function generateRoomCode() {
    const code = Math.floor(10000 + Math.random() * 90000).toString();
    return rooms[code] ? generateRoomCode() : code;
}

function createRoom(leaderId, leaderName) {
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
    rooms[code].players[leaderId] = {
        id: leaderId,
        name: leaderName,
        ready: true,  // Leader is auto-ready
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
        input: { w: false, a: false, s: false, d: false, angle: 0, charging: false, seq: 0 },
        lastUpdate: Date.now()
    };
    
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
        }
    });
}

// ==================== SOCKET.IO EVENTS ====================
io.on('connection', (socket) => {
    console.log(`Player connected: ${socket.id}`);
    
    let currentRoom = null;
    let heartbeatInterval = null;
    
    // Heartbeat
    heartbeatInterval = setInterval(() => {
        socket.emit('heartbeat');
    }, 5000);

    function removePlayerFromRoom() {
        const roomCode = socket.roomCode;
        if (!roomCode || !rooms[roomCode]) return;

        const room = rooms[roomCode];
        const disconnectedPlayer = room.players[socket.id];

        if (disconnectedPlayer) {
            console.log(`Player ${disconnectedPlayer.name} left room ${roomCode}`);
        }

        delete room.players[socket.id];

        const remainingPlayers = Object.values(room.players);

        if (remainingPlayers.length === 0) {
            delete rooms[roomCode];
            console.log(`Room ${roomCode} deleted (empty)`);
            return;
        }

        if (room.leader === socket.id) {
            const newLeader = remainingPlayers[0];
            room.leader = newLeader.id;
            newLeader.ready = true;

            console.log(`New leader: ${newLeader.name}`);
            io.to(roomCode).emit('newLeader', {
                leaderId: newLeader.id,
                leaderName: newLeader.name
            });
        }

        emitLobbyUpdate(roomCode);

        io.to(roomCode).emit('playerLeft', {
            playerId: socket.id,
            playerName: disconnectedPlayer ? disconnectedPlayer.name : 'Unknown'
        });
    }
    
    // داخل io.on('connection', ...) في ملف server.js
    socket.on('registerPlayer', (data) => {
        socket.persistentId = data.id;
        socket.playerName = data.name;
        console.log(`Player recognized: ${data.name} with ID: ${data.id}`);
    });

    socket.on('pong', () => {
        if (currentRoom && rooms[currentRoom] && rooms[currentRoom].players[socket.id]) {
            rooms[currentRoom].players[socket.id].lastUpdate = Date.now();
        }
    });
    
    // Create room
    socket.on('createRoom', (data) => {
    // إنشاء الغرفة باستخدام الاسم القادم من المتصفح
        const code = createRoom(socket.id, data.playerName || 'Soldier');
        currentRoom = code;
        socket.join(code);

        socket.roomCode = code;
        
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
        
        room.players[socket.id] = {
            id: socket.id,
            name: data.playerName || 'Player',
            ready: false,
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
            input: { w: false, a: false, s: false, d: false, angle: 0, charging: false, seq: 0 },
            lastUpdate: Date.now()
        };
        
        io.to(code).emit('playerJoined', {
            roomCode: code,
            players: room.players,
            leaderId: room.leader
        });

        emitLobbyUpdate(code);
        
        console.log(`Player ${socket.id} joined room ${code}`);
    });
    
    socket.on('playerReady', () => {
        // استخدم socket.roomCode بدلاً من البحث
        const roomCode = socket.roomCode;
        const room = rooms[roomCode];
        
        if (!room || !room.players[socket.id]) {
            console.log('Player not in any room');
            return;
        }
        
        const player = room.players[socket.id];
        
        // لا تسمح للقائد بتغيير ready status
        if (room.leader === socket.id) {
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
        if (!currentRoom || !rooms[currentRoom]) return;
        
        const player = rooms[currentRoom].players[socket.id];
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
        const roomCode = socket.roomCode;
        const room = rooms[roomCode];
        if (!room || room.state !== 'playing') return;
        const player = room.players[socket.id];
        if (!player || player.hp <= 0) return;

        player.input = {
            w: !!data.w,
            a: !!data.a,
            s: !!data.s,
            d: !!data.d,
            angle: typeof data.angle === 'number' ? data.angle : player.angle,
            charging: !!data.charging,
            seq: data.seq || 0
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
        if (!currentRoom || !rooms[currentRoom] || rooms[currentRoom].state !== 'playing') return;
        
        const player = rooms[currentRoom].players[socket.id];
        if (!player || player.hp <= 0) return;

        if (player.invisible) {
            player.invisible = false;
            player.invisExpire = 0;
        }
        
        const angle = typeof data.angle === 'number' ? data.angle : player.angle;
        const speed = 871.2; // 14.52 * 60fps
        const offset = 25;
        const projectile = {
            id: `proj_${socket.id}_${Date.now()}`,
            ownerId: socket.id,
            x: player.x + Math.cos(angle) * offset,
            y: player.y + Math.sin(angle) * offset,
            vx: Math.cos(angle) * speed,
            vy: Math.sin(angle) * speed,
            angle: angle,
            life: 0
        };
        
        rooms[currentRoom].projectiles.push(projectile);
        
        io.to(currentRoom).emit('projectileFired', projectile);
    });

    socket.on('leaveRoom', () => {
        if (!socket.roomCode) return;
        const roomCode = socket.roomCode;
        socket.leave(roomCode);
        removePlayerFromRoom();
        socket.roomCode = null;
        currentRoom = null;
    });
    
    // Disconnect
   // استبدل السطر 563-589 بهذا:
socket.on('disconnect', () => {
    console.log(`Player disconnected: ${socket.id}`);
    removePlayerFromRoom();
    
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
                room.state = 'finished';
                io.to(roomCode).emit('gameEnd', {
                    players: room.players
                });
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

