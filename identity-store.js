const crypto = require('crypto');

let PgPool = null;
try {
    PgPool = require('pg').Pool;
} catch (_) {
    PgPool = null;
}

const FRIEND_CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const FRIEND_CODE_PREFIX = 'HS';
const FRIEND_CODE_LEN = 6;
const DEFAULT_OTP_LEN = 6;

function randomUuid() {
    if (typeof crypto.randomUUID === 'function') return crypto.randomUUID();
    const b = crypto.randomBytes(16);
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    const h = b.toString('hex');
    return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

function makeFriendCode() {
    let out = FRIEND_CODE_PREFIX;
    for (let i = 0; i < FRIEND_CODE_LEN; i++) {
        const idx = Math.floor(Math.random() * FRIEND_CODE_CHARS.length);
        out += FRIEND_CODE_CHARS[idx];
    }
    return out;
}

function makeOtpCode(len = DEFAULT_OTP_LEN) {
    let out = '';
    for (let i = 0; i < len; i++) {
        out += Math.floor(Math.random() * 10).toString();
    }
    return out;
}

function hashOtp(code) {
    return crypto.createHash('sha256').update(String(code || '')).digest('hex');
}

function timingSafeHexEqual(a, b) {
    if (!a || !b || typeof a !== 'string' || typeof b !== 'string') return false;
    const aa = Buffer.from(a, 'hex');
    const bb = Buffer.from(b, 'hex');
    if (!aa.length || !bb.length || aa.length !== bb.length) return false;
    return crypto.timingSafeEqual(aa, bb);
}

function sanitizeNickname(raw) {
    const base = (typeof raw === 'string' ? raw : '').trim();
    if (!base) return 'Player';
    return base.slice(0, 32);
}

function normalizeEmail(raw) {
    return String(raw || '').trim().toLowerCase();
}

function normalizeUsername(raw) {
    return String(raw || '').trim().toLowerCase();
}

function normalizeFriendCode(raw) {
    return String(raw || '').trim().toUpperCase();
}

function friendPairKey(a, b) {
    const x = String(a || '');
    const y = String(b || '');
    if (!x || !y) return '';
    return x < y ? `${x}|${y}` : `${y}|${x}`;
}

function toPublicProfile(profile, account) {
    if (!profile) return null;
    const accountStatus = account ? account.status : null;
    const isGuest = accountStatus !== 'active';
    return {
        profileId: profile.id,
        nickname: profile.nickname,
        friendCode: profile.friendCode,
        username: !isGuest && account ? account.username : null,
        isGuest
    };
}

function toMs(v) {
    if (v instanceof Date) return v.getTime();
    if (typeof v === 'number') return v;
    return Date.parse(v);
}

function storeError(code, message, extra = {}) {
    const err = new Error(message || code);
    err.code = code;
    Object.assign(err, extra);
    return err;
}

function buildPgConfig(databaseUrl) {
    const sslMode = String(process.env.DATABASE_SSL || '').toLowerCase();
    let ssl = false;
    if (sslMode === 'require' || sslMode === 'true' || sslMode === '1') {
        ssl = { rejectUnauthorized: false };
    } else if (sslMode === 'disable' || sslMode === 'false' || sslMode === '0') {
        ssl = false;
    } else if (String(process.env.NODE_ENV || '').toLowerCase() === 'production') {
        ssl = { rejectUnauthorized: false };
    }
    return {
        connectionString: databaseUrl,
        ssl
    };
}

class MemoryIdentityStore {
    constructor() {
        this.mode = 'memory';
        this.deviceActiveProfile = new Map(); // persistentId -> profileId
        this.deviceProfiles = new Map(); // persistentId -> Set(profileId)
        this.profilesById = new Map(); // profileId -> { id, nickname, friendCode, createdAt, updatedAt }
        this.friendCodeToId = new Map();
        this.accountsById = new Map(); // accountId -> account row
        this.accountByEmailNorm = new Map(); // email_norm -> accountId
        this.accountByUsernameNorm = new Map(); // username_norm -> accountId
        this.accountLinkByProfile = new Map(); // profileId -> accountId
        this.accountLinkByAccount = new Map(); // accountId -> profileId
        this.emailChallengesByAccount = new Map(); // accountId -> challenge
        this.friendRequestsById = new Map(); // requestId -> row
        this.friendRequestsByToProfile = new Map(); // toProfileId -> Set(requestId)
        this.friendRequestsByFromProfile = new Map(); // fromProfileId -> Set(requestId)
        this.friendshipsByPair = new Map(); // pairKey -> row
        this.friendsByProfile = new Map(); // profileId -> Set(profileId)
    }

    async init() {
        return true;
    }

    describe() {
        return {
            mode: this.mode,
            profiles: this.profilesById.size,
            devices: this.deviceActiveProfile.size,
            accounts: this.accountsById.size
        };
    }

    touchDeviceProfile(persistentId, profileId) {
        if (!this.deviceProfiles.has(persistentId)) {
            this.deviceProfiles.set(persistentId, new Set());
        }
        this.deviceProfiles.get(persistentId).add(profileId);
    }

    touchFriendRequestIndex(request) {
        if (!request || !request.id) return;
        if (!this.friendRequestsByToProfile.has(request.toProfileId)) {
            this.friendRequestsByToProfile.set(request.toProfileId, new Set());
        }
        if (!this.friendRequestsByFromProfile.has(request.fromProfileId)) {
            this.friendRequestsByFromProfile.set(request.fromProfileId, new Set());
        }
        this.friendRequestsByToProfile.get(request.toProfileId).add(request.id);
        this.friendRequestsByFromProfile.get(request.fromProfileId).add(request.id);
    }

    ensureFriendsSet(profileId) {
        if (!this.friendsByProfile.has(profileId)) {
            this.friendsByProfile.set(profileId, new Set());
        }
        return this.friendsByProfile.get(profileId);
    }

    addFriendship(profileA, profileB, createdAt = Date.now()) {
        const pair = friendPairKey(profileA, profileB);
        if (!pair) return null;
        if (this.friendshipsByPair.has(pair)) return this.friendshipsByPair.get(pair);
        const row = {
            id: randomUuid(),
            profileA,
            profileB,
            createdAt
        };
        this.friendshipsByPair.set(pair, row);
        this.ensureFriendsSet(profileA).add(profileB);
        this.ensureFriendsSet(profileB).add(profileA);
        return row;
    }

    areFriends(profileA, profileB) {
        const pair = friendPairKey(profileA, profileB);
        if (!pair) return false;
        return this.friendshipsByPair.has(pair);
    }

    getProfilePublic(profileId) {
        const profile = this.profilesById.get(profileId);
        if (!profile) return null;
        const account = this.resolveAccountForProfile(profileId);
        return toPublicProfile(profile, account);
    }

    findPendingRequestBetween(profileA, profileB) {
        let found = null;
        this.friendRequestsById.forEach((req) => {
            if (found) return;
            if (!req || req.status !== 'pending') return;
            const sameDir = req.fromProfileId === profileA && req.toProfileId === profileB;
            const revDir = req.fromProfileId === profileB && req.toProfileId === profileA;
            if (sameDir || revDir) found = req;
        });
        return found;
    }

    listPendingRequests(profileId, kind) {
        const index = kind === 'incoming' ? this.friendRequestsByToProfile : this.friendRequestsByFromProfile;
        const ids = index.get(profileId) || new Set();
        const out = [];
        ids.forEach((id) => {
            const req = this.friendRequestsById.get(id);
            if (!req || req.status !== 'pending') return;
            out.push(req);
        });
        out.sort((a, b) => Number(b.createdAt || 0) - Number(a.createdAt || 0));
        return out;
    }

    resolveAccountForProfile(profileId) {
        const accountId = this.accountLinkByProfile.get(profileId);
        if (!accountId) return null;
        return this.accountsById.get(accountId) || null;
    }

    buildProfileSnapshot(profileId) {
        const profile = this.profilesById.get(profileId);
        if (!profile) return null;
        const account = this.resolveAccountForProfile(profileId);
        const accountStatus = account ? account.status : null;
        const isGuest = accountStatus !== 'active';
        return {
            id: profile.id,
            nickname: profile.nickname,
            friendCode: profile.friendCode,
            isGuest,
            username: !isGuest && account ? account.username : null,
            accountStatus
        };
    }

    async getProfileSnapshotById(profileId) {
        return this.buildProfileSnapshot(profileId);
    }

    createGuestProfileForDevice(persistentId, nickname) {
        const profileId = randomUuid();
        let friendCode = '';
        for (let i = 0; i < 16; i++) {
            const candidate = makeFriendCode();
            if (!this.friendCodeToId.has(candidate)) {
                friendCode = candidate;
                break;
            }
        }
        if (!friendCode) throw storeError('FRIEND_CODE_GENERATION_FAILED', 'friend code generation failed');

        const now = Date.now();
        const profile = {
            id: profileId,
            nickname,
            friendCode,
            createdAt: now,
            updatedAt: now
        };
        this.profilesById.set(profileId, profile);
        this.friendCodeToId.set(friendCode, profileId);
        this.deviceActiveProfile.set(persistentId, profileId);
        this.touchDeviceProfile(persistentId, profileId);
        return profileId;
    }

    async setActiveProfileForDevice(params) {
        const persistentId = String(params && params.persistentId ? params.persistentId : '').trim();
        const profileId = String(params && params.profileId ? params.profileId : '').trim();
        if (!persistentId || !profileId) throw storeError('INVALID_DEVICE_OR_PROFILE', 'invalid device/profile');
        if (!this.profilesById.has(profileId)) throw storeError('PROFILE_NOT_FOUND', 'profile not found');
        this.deviceActiveProfile.set(persistentId, profileId);
        this.touchDeviceProfile(persistentId, profileId);
        return this.buildProfileSnapshot(profileId);
    }

    async ensureGuestProfile(params) {
        const persistentId = String(params && params.persistentId ? params.persistentId : '').trim();
        if (!persistentId) throw storeError('MISSING_PERSISTENT_ID', 'missing persistentId');
        const nickname = sanitizeNickname(params && params.nickname);
        const currentProfileId = this.deviceActiveProfile.get(persistentId);
        if (currentProfileId && this.profilesById.has(currentProfileId)) {
            const profile = this.profilesById.get(currentProfileId);
            if (nickname && nickname !== profile.nickname) {
                profile.nickname = nickname;
                profile.updatedAt = Date.now();
            }
            this.touchDeviceProfile(persistentId, currentProfileId);
            return this.buildProfileSnapshot(currentProfileId);
        }

        const profileId = this.createGuestProfileForDevice(persistentId, nickname);
        return this.buildProfileSnapshot(profileId);
    }

    async createPendingLinkedAccount(params) {
        const profileId = String(params && params.profileId ? params.profileId : '').trim();
        const email = String(params && params.email ? params.email : '').trim();
        const username = String(params && params.username ? params.username : '').trim();
        const passwordHash = String(params && params.passwordHash ? params.passwordHash : '').trim();
        const codeTtlMs = Number(params && params.codeTtlMs) || (10 * 60 * 1000);
        const now = Date.now();

        const emailNorm = normalizeEmail(email);
        const usernameNorm = normalizeUsername(username);
        if (!profileId || !this.profilesById.has(profileId)) throw storeError('PROFILE_NOT_FOUND', 'profile not found');
        if (!emailNorm) throw storeError('INVALID_EMAIL', 'invalid email');
        if (!usernameNorm) throw storeError('INVALID_USERNAME', 'invalid username');
        if (!passwordHash) throw storeError('INVALID_PASSWORD_HASH', 'invalid password hash');
        if (this.accountLinkByProfile.has(profileId)) throw storeError('PROFILE_ALREADY_LINKED', 'profile already linked');
        if (this.accountByEmailNorm.has(emailNorm)) throw storeError('EMAIL_ALREADY_USED', 'email already used');
        if (this.accountByUsernameNorm.has(usernameNorm)) throw storeError('USERNAME_TAKEN', 'username already used');

        const accountId = randomUuid();
        const account = {
            id: accountId,
            email,
            emailNorm,
            username,
            usernameNorm,
            passwordHash,
            status: 'pending_verification',
            emailVerifiedAt: null,
            createdAt: now,
            updatedAt: now
        };
        this.accountsById.set(accountId, account);
        this.accountByEmailNorm.set(emailNorm, accountId);
        this.accountByUsernameNorm.set(usernameNorm, accountId);
        this.accountLinkByProfile.set(profileId, accountId);
        this.accountLinkByAccount.set(accountId, profileId);

        const code = makeOtpCode();
        const challenge = {
            id: randomUuid(),
            accountId,
            codeHash: hashOtp(code),
            expiresAt: now + codeTtlMs,
            usedAt: null,
            attemptCount: 0,
            lastSentAt: now,
            sendCount: 1,
            sendWindowStart: now,
            createdAt: now,
            updatedAt: now
        };
        this.emailChallengesByAccount.set(accountId, challenge);

        return {
            accountId,
            profileId,
            email,
            username,
            verificationCode: code,
            expiresAt: challenge.expiresAt
        };
    }

    async resendVerification(params) {
        const emailNorm = normalizeEmail(params && params.email);
        const codeTtlMs = Number(params && params.codeTtlMs) || (10 * 60 * 1000);
        const resendCooldownMs = Number(params && params.resendCooldownMs) || (60 * 1000);
        const maxSendsPerHour = Number(params && params.maxSendsPerHour) || 5;
        const now = Date.now();
        if (!emailNorm) throw storeError('INVALID_EMAIL', 'invalid email');

        const accountId = this.accountByEmailNorm.get(emailNorm);
        if (!accountId) throw storeError('ACCOUNT_NOT_FOUND', 'account not found');
        const account = this.accountsById.get(accountId);
        if (!account) throw storeError('ACCOUNT_NOT_FOUND', 'account not found');
        if (account.status === 'active') throw storeError('ACCOUNT_ALREADY_ACTIVE', 'account already active');

        let challenge = this.emailChallengesByAccount.get(accountId);
        if (!challenge) {
            challenge = {
                id: randomUuid(),
                accountId,
                codeHash: '',
                expiresAt: now,
                usedAt: null,
                attemptCount: 0,
                lastSentAt: 0,
                sendCount: 0,
                sendWindowStart: now,
                createdAt: now,
                updatedAt: now
            };
            this.emailChallengesByAccount.set(accountId, challenge);
        }

        if (now - challenge.lastSentAt < resendCooldownMs) {
            throw storeError('VERIFICATION_RATE_LIMITED', 'verification resend cooldown', {
                retryAfterMs: resendCooldownMs - (now - challenge.lastSentAt)
            });
        }
        if (now - challenge.sendWindowStart >= (60 * 60 * 1000)) {
            challenge.sendWindowStart = now;
            challenge.sendCount = 0;
        }
        if (challenge.sendCount >= maxSendsPerHour) {
            throw storeError('VERIFICATION_RATE_LIMITED', 'verification resend rate limited');
        }

        const code = makeOtpCode();
        challenge.codeHash = hashOtp(code);
        challenge.expiresAt = now + codeTtlMs;
        challenge.usedAt = null;
        challenge.attemptCount = 0;
        challenge.lastSentAt = now;
        challenge.sendCount += 1;
        challenge.updatedAt = now;

        return {
            accountId,
            email: account.email,
            verificationCode: code,
            expiresAt: challenge.expiresAt
        };
    }

    async verifyEmailCode(params) {
        const emailNorm = normalizeEmail(params && params.email);
        const otp = String(params && params.otp ? params.otp : '').trim();
        const maxVerifyAttempts = Number(params && params.maxVerifyAttempts) || 8;
        const now = Date.now();
        if (!emailNorm) throw storeError('INVALID_EMAIL', 'invalid email');
        if (!otp) throw storeError('INVALID_VERIFICATION_CODE', 'invalid verification code');

        const accountId = this.accountByEmailNorm.get(emailNorm);
        if (!accountId) throw storeError('ACCOUNT_NOT_FOUND', 'account not found');
        const account = this.accountsById.get(accountId);
        if (!account) throw storeError('ACCOUNT_NOT_FOUND', 'account not found');
        if (account.status === 'active') throw storeError('ACCOUNT_ALREADY_ACTIVE', 'account already active');
        if (account.status !== 'pending_verification') throw storeError('EMAIL_NOT_VERIFIED', 'account not pending verification');

        const challenge = this.emailChallengesByAccount.get(accountId);
        if (!challenge) throw storeError('VERIFICATION_CODE_EXPIRED', 'verification challenge not found');
        if (challenge.usedAt) throw storeError('VERIFICATION_CODE_EXPIRED', 'verification challenge already used');
        if (now > challenge.expiresAt) throw storeError('VERIFICATION_CODE_EXPIRED', 'verification challenge expired');
        if (challenge.attemptCount >= maxVerifyAttempts) {
            throw storeError('VERIFICATION_RATE_LIMITED', 'verification attempts exceeded');
        }

        const givenHash = hashOtp(otp);
        if (!timingSafeHexEqual(challenge.codeHash, givenHash)) {
            challenge.attemptCount += 1;
            challenge.updatedAt = now;
            throw storeError('INVALID_VERIFICATION_CODE', 'invalid verification code');
        }

        challenge.usedAt = now;
        challenge.updatedAt = now;
        account.status = 'active';
        account.emailVerifiedAt = now;
        account.updatedAt = now;
        const profileId = this.accountLinkByAccount.get(accountId) || null;

        return {
            accountId: account.id,
            email: account.email,
            username: account.username,
            profileId
        };
    }

    async findAccountByLogin(params) {
        const login = String(params && params.login ? params.login : '').trim();
        if (!login) return null;
        const norm = login.includes('@') ? normalizeEmail(login) : normalizeUsername(login);
        const accountId = login.includes('@')
            ? this.accountByEmailNorm.get(norm)
            : (this.accountByUsernameNorm.get(norm) || this.accountByEmailNorm.get(normalizeEmail(login)));
        if (!accountId) return null;
        const account = this.accountsById.get(accountId);
        if (!account) return null;
        const profileId = this.accountLinkByAccount.get(accountId) || null;
        const profile = profileId ? this.profilesById.get(profileId) : null;
        return {
            accountId: account.id,
            email: account.email,
            username: account.username,
            passwordHash: account.passwordHash,
            status: account.status,
            profileId,
            profileNickname: profile ? profile.nickname : null,
            profileFriendCode: profile ? profile.friendCode : null
        };
    }

    async switchToGuestProfileForDevice(params) {
        const persistentId = String(params && params.persistentId ? params.persistentId : '').trim();
        const fallbackNickname = sanitizeNickname(params && params.nickname);
        if (!persistentId) throw storeError('MISSING_PERSISTENT_ID', 'missing persistentId');

        const profileIds = this.deviceProfiles.get(persistentId) || new Set();
        let best = null;
        profileIds.forEach((id) => {
            const profile = this.profilesById.get(id);
            if (!profile) return;
            const account = this.resolveAccountForProfile(id);
            if (account && account.status === 'active') return;
            if (!best || profile.updatedAt > best.updatedAt) best = profile;
        });

        if (!best) {
            const profileId = this.createGuestProfileForDevice(persistentId, fallbackNickname);
            return this.buildProfileSnapshot(profileId);
        }
        this.deviceActiveProfile.set(persistentId, best.id);
        this.touchDeviceProfile(persistentId, best.id);
        return this.buildProfileSnapshot(best.id);
    }

    buildFriendsState(profileId) {
        const me = this.getProfilePublic(profileId);
        if (!me) throw storeError('PROFILE_NOT_FOUND', 'profile not found');

        const friendIds = Array.from(this.friendsByProfile.get(profileId) || []);
        const friends = friendIds
            .map((id) => {
                const p = this.getProfilePublic(id);
                if (!p) return null;
                return {
                    profileId: p.profileId,
                    nickname: p.nickname,
                    username: p.username,
                    friendCode: p.friendCode,
                    isGuest: p.isGuest
                };
            })
            .filter(Boolean)
            .sort((a, b) => String(a.nickname || '').localeCompare(String(b.nickname || '')));

        const incoming = this.listPendingRequests(profileId, 'incoming')
            .map((req) => {
                const from = this.getProfilePublic(req.fromProfileId);
                if (!from) return null;
                return {
                    requestId: req.id,
                    fromProfileId: req.fromProfileId,
                    toProfileId: req.toProfileId,
                    nickname: from.nickname,
                    username: from.username,
                    friendCode: from.friendCode,
                    createdAt: req.createdAt
                };
            })
            .filter(Boolean);

        const outgoing = this.listPendingRequests(profileId, 'outgoing')
            .map((req) => {
                const to = this.getProfilePublic(req.toProfileId);
                if (!to) return null;
                return {
                    requestId: req.id,
                    fromProfileId: req.fromProfileId,
                    toProfileId: req.toProfileId,
                    nickname: to.nickname,
                    username: to.username,
                    friendCode: to.friendCode,
                    createdAt: req.createdAt
                };
            })
            .filter(Boolean);

        return {
            profileId: me.profileId,
            friends,
            incoming,
            outgoing,
            incomingCount: incoming.length
        };
    }

    async getFriendsState(params) {
        const profileId = String(params && params.profileId ? params.profileId : '').trim();
        if (!profileId) throw storeError('PROFILE_NOT_FOUND', 'profile not found');
        return this.buildFriendsState(profileId);
    }

    async searchFriendProfiles(params) {
        const profileId = String(params && params.profileId ? params.profileId : '').trim();
        const query = String(params && params.query ? params.query : '').trim();
        const limit = Math.max(1, Math.min(20, Number(params && params.limit) || 10));
        if (!profileId || !this.profilesById.has(profileId)) throw storeError('PROFILE_NOT_FOUND', 'profile not found');
        if (!query) return { results: [] };

        const queryFriendCode = normalizeFriendCode(query);
        const queryUsername = normalizeUsername(query);
        const candidateIds = [];
        const byCode = this.friendCodeToId.get(queryFriendCode);
        if (byCode) candidateIds.push(byCode);
        const accountId = this.accountByUsernameNorm.get(queryUsername);
        if (accountId) {
            const pid = this.accountLinkByAccount.get(accountId);
            if (pid) candidateIds.push(pid);
        }

        const dedup = [];
        const seen = new Set();
        candidateIds.forEach((id) => {
            if (!id || seen.has(id)) return;
            seen.add(id);
            dedup.push(id);
        });

        const results = dedup.slice(0, limit).map((candidateId) => {
            const pub = this.getProfilePublic(candidateId);
            if (!pub) return null;
            const self = candidateId === profileId;
            const alreadyFriends = !self && this.areFriends(profileId, candidateId);
            const pending = !self ? this.findPendingRequestBetween(profileId, candidateId) : null;
            const pendingOutgoing = !!(pending && pending.fromProfileId === profileId && pending.toProfileId === candidateId);
            const pendingIncoming = !!(pending && pending.fromProfileId === candidateId && pending.toProfileId === profileId);
            return {
                profileId: pub.profileId,
                nickname: pub.nickname,
                username: pub.username,
                friendCode: pub.friendCode,
                isGuest: pub.isGuest,
                eligibility: {
                    isSelf: self,
                    alreadyFriends,
                    pendingOutgoing,
                    pendingIncoming,
                    canRequest: !(self || alreadyFriends || pendingOutgoing || pendingIncoming)
                }
            };
        }).filter(Boolean);

        return { results };
    }

    async sendFriendRequest(params) {
        const fromProfileId = String(params && params.fromProfileId ? params.fromProfileId : '').trim();
        const toProfileId = String(params && params.toProfileId ? params.toProfileId : '').trim();
        if (!fromProfileId || !toProfileId) throw storeError('PROFILE_NOT_FOUND', 'profile not found');
        if (!this.profilesById.has(fromProfileId) || !this.profilesById.has(toProfileId)) {
            throw storeError('PROFILE_NOT_FOUND', 'profile not found');
        }
        if (fromProfileId === toProfileId) {
            throw storeError('FRIEND_REQUEST_NOT_ALLOWED', 'cannot request self');
        }
        if (this.areFriends(fromProfileId, toProfileId)) {
            throw storeError('ALREADY_FRIENDS', 'already friends');
        }
        const pending = this.findPendingRequestBetween(fromProfileId, toProfileId);
        if (pending) {
            throw storeError('FRIEND_REQUEST_ALREADY_EXISTS', 'friend request already exists');
        }

        const now = Date.now();
        const req = {
            id: randomUuid(),
            fromProfileId,
            toProfileId,
            status: 'pending',
            createdAt: now,
            respondedAt: null
        };
        this.friendRequestsById.set(req.id, req);
        this.touchFriendRequestIndex(req);
        return {
            requestId: req.id,
            fromProfileId,
            toProfileId,
            status: req.status,
            createdAt: req.createdAt
        };
    }

    async respondFriendRequest(params) {
        const profileId = String(params && params.profileId ? params.profileId : '').trim();
        const requestId = String(params && params.requestId ? params.requestId : '').trim();
        const accept = !!(params && params.accept);
        if (!profileId || !this.profilesById.has(profileId)) throw storeError('PROFILE_NOT_FOUND', 'profile not found');
        const req = this.friendRequestsById.get(requestId);
        if (!req) throw storeError('FRIEND_REQUEST_NOT_FOUND', 'friend request not found');
        if (req.toProfileId !== profileId) throw storeError('FRIEND_REQUEST_NOT_ALLOWED', 'friend request not allowed');
        if (req.status !== 'pending') throw storeError('FRIEND_REQUEST_NOT_PENDING', 'friend request not pending');

        const now = Date.now();
        req.status = accept ? 'accepted' : 'rejected';
        req.respondedAt = now;
        if (accept) {
            this.addFriendship(req.fromProfileId, req.toProfileId, now);
        }
        return {
            requestId: req.id,
            fromProfileId: req.fromProfileId,
            toProfileId: req.toProfileId,
            status: req.status,
            respondedAt: req.respondedAt
        };
    }

    async close() {
        return true;
    }
}

class PostgresIdentityStore {
    constructor(databaseUrl) {
        this.mode = 'postgres';
        this.databaseUrl = databaseUrl;
        this.pool = null;
    }

    async init() {
        if (!PgPool) throw new Error('pg dependency is not installed');
        this.pool = new PgPool(buildPgConfig(this.databaseUrl));
        await this.pool.query(`
            CREATE TABLE IF NOT EXISTS user_profiles (
                id UUID PRIMARY KEY,
                nickname VARCHAR(32) NOT NULL DEFAULT 'Player',
                friend_code VARCHAR(12) NOT NULL UNIQUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);
        await this.pool.query(`
            CREATE TABLE IF NOT EXISTS devices (
                persistent_id VARCHAR(128) PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);
        await this.pool.query(`
            CREATE TABLE IF NOT EXISTS device_profiles (
                device_persistent_id VARCHAR(128) NOT NULL REFERENCES devices(persistent_id) ON DELETE CASCADE,
                profile_id UUID NOT NULL REFERENCES user_profiles(id) ON DELETE CASCADE,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (device_persistent_id, profile_id)
            );
        `);
        await this.pool.query(`
            CREATE UNIQUE INDEX IF NOT EXISTS idx_device_profiles_active_device
            ON device_profiles (device_persistent_id)
            WHERE is_active = TRUE;
        `);
        await this.pool.query(`
            CREATE INDEX IF NOT EXISTS idx_device_profiles_profile
            ON device_profiles (profile_id);
        `);
        await this.pool.query(`
            CREATE TABLE IF NOT EXISTS accounts (
                id UUID PRIMARY KEY,
                email VARCHAR(320) NOT NULL,
                email_norm VARCHAR(320) NOT NULL UNIQUE,
                username VARCHAR(32) NOT NULL,
                username_norm VARCHAR(32) NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                status VARCHAR(32) NOT NULL,
                email_verified_at TIMESTAMPTZ NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CHECK (status IN ('pending_verification', 'active', 'suspended'))
            );
        `);
        await this.pool.query(`
            CREATE TABLE IF NOT EXISTS account_profile_links (
                account_id UUID NOT NULL UNIQUE REFERENCES accounts(id) ON DELETE CASCADE,
                profile_id UUID NOT NULL UNIQUE REFERENCES user_profiles(id) ON DELETE CASCADE,
                linked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (account_id, profile_id)
            );
        `);
        await this.pool.query(`
            CREATE TABLE IF NOT EXISTS email_verification_challenges (
                id UUID PRIMARY KEY,
                account_id UUID NOT NULL UNIQUE REFERENCES accounts(id) ON DELETE CASCADE,
                code_hash TEXT NOT NULL,
                expires_at TIMESTAMPTZ NOT NULL,
                used_at TIMESTAMPTZ NULL,
                attempt_count INT NOT NULL DEFAULT 0,
                last_sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                send_count INT NOT NULL DEFAULT 1,
                send_window_start TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);
        await this.pool.query(`
            CREATE TABLE IF NOT EXISTS friend_requests (
                id UUID PRIMARY KEY,
                from_profile_id UUID NOT NULL REFERENCES user_profiles(id) ON DELETE CASCADE,
                to_profile_id UUID NOT NULL REFERENCES user_profiles(id) ON DELETE CASCADE,
                status VARCHAR(16) NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                responded_at TIMESTAMPTZ NULL,
                CHECK (status IN ('pending', 'accepted', 'rejected', 'cancelled', 'expired')),
                CHECK (from_profile_id <> to_profile_id)
            );
        `);
        await this.pool.query(`
            CREATE INDEX IF NOT EXISTS idx_friend_requests_to_pending
            ON friend_requests (to_profile_id, created_at DESC)
            WHERE status = 'pending';
        `);
        await this.pool.query(`
            CREATE INDEX IF NOT EXISTS idx_friend_requests_from_pending
            ON friend_requests (from_profile_id, created_at DESC)
            WHERE status = 'pending';
        `);
        await this.pool.query(`
            CREATE TABLE IF NOT EXISTS friendships (
                id UUID PRIMARY KEY,
                profile_a UUID NOT NULL REFERENCES user_profiles(id) ON DELETE CASCADE,
                profile_b UUID NOT NULL REFERENCES user_profiles(id) ON DELETE CASCADE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CHECK (profile_a <> profile_b)
            );
        `);
        await this.pool.query(`
            CREATE UNIQUE INDEX IF NOT EXISTS idx_friendships_pair_unique
            ON friendships (LEAST(profile_a, profile_b), GREATEST(profile_a, profile_b));
        `);
        await this.pool.query(`
            CREATE UNIQUE INDEX IF NOT EXISTS idx_friend_requests_pending_pair_unique
            ON friend_requests (LEAST(from_profile_id, to_profile_id), GREATEST(from_profile_id, to_profile_id))
            WHERE status = 'pending';
        `);
        return true;
    }

    describe() {
        return { mode: this.mode };
    }

    async getProfileSnapshotById(profileId, client = null) {
        const runner = client || this.pool;
        const rs = await runner.query(
            `SELECT
                up.id,
                up.nickname,
                up.friend_code,
                a.username,
                a.status AS account_status
             FROM user_profiles up
             LEFT JOIN account_profile_links apl ON apl.profile_id = up.id
             LEFT JOIN accounts a ON a.id = apl.account_id
             WHERE up.id = $1
             LIMIT 1`,
            [profileId]
        );
        if (!rs.rowCount) return null;
        const row = rs.rows[0];
        const accountStatus = row.account_status || null;
        const isGuest = accountStatus !== 'active';
        return {
            id: row.id,
            nickname: row.nickname,
            friendCode: row.friend_code,
            isGuest,
            username: !isGuest ? row.username : null,
            accountStatus
        };
    }

    async setActiveProfileForDeviceTx(client, persistentId, profileId) {
        await client.query(
            `INSERT INTO devices (persistent_id, last_seen_at)
             VALUES ($1, NOW())
             ON CONFLICT (persistent_id)
             DO UPDATE SET last_seen_at = NOW()`,
            [persistentId]
        );
        await client.query(
            `UPDATE device_profiles
             SET is_active = FALSE, updated_at = NOW()
             WHERE device_persistent_id = $1 AND is_active = TRUE`,
            [persistentId]
        );
        await client.query(
            `INSERT INTO device_profiles (device_persistent_id, profile_id, is_active, updated_at)
             VALUES ($1, $2, TRUE, NOW())
             ON CONFLICT (device_persistent_id, profile_id)
             DO UPDATE SET is_active = TRUE, updated_at = NOW()`,
            [persistentId, profileId]
        );
    }

    async setActiveProfileForDevice(params) {
        const persistentId = String(params && params.persistentId ? params.persistentId : '').trim();
        const profileId = String(params && params.profileId ? params.profileId : '').trim();
        if (!persistentId || !profileId) throw storeError('INVALID_DEVICE_OR_PROFILE', 'invalid device/profile');
        const client = await this.pool.connect();
        try {
            await client.query('BEGIN');
            const exists = await client.query(`SELECT id FROM user_profiles WHERE id = $1 LIMIT 1`, [profileId]);
            if (!exists.rowCount) throw storeError('PROFILE_NOT_FOUND', 'profile not found');
            await this.setActiveProfileForDeviceTx(client, persistentId, profileId);
            const snapshot = await this.getProfileSnapshotById(profileId, client);
            await client.query('COMMIT');
            return snapshot;
        } catch (err) {
            try { await client.query('ROLLBACK'); } catch (_) {}
            throw err;
        } finally {
            client.release();
        }
    }

    async verifyEmailCode(params) {
        const emailNorm = normalizeEmail(params && params.email);
        const otp = String(params && params.otp ? params.otp : '').trim();
        const maxVerifyAttempts = Number(params && params.maxVerifyAttempts) || 8;
        if (!emailNorm) throw storeError('INVALID_EMAIL', 'invalid email');
        if (!otp) throw storeError('INVALID_VERIFICATION_CODE', 'invalid verification code');
        const now = Date.now();
        const nowSec = now / 1000;

        const client = await this.pool.connect();
        let finalized = false;
        try {
            await client.query('BEGIN');
            const accountRs = await client.query(
                `SELECT id, email, username, status
                 FROM accounts
                 WHERE email_norm = $1
                 LIMIT 1
                 FOR UPDATE`,
                [emailNorm]
            );
            if (!accountRs.rowCount) throw storeError('ACCOUNT_NOT_FOUND', 'account not found');
            const account = accountRs.rows[0];
            if (account.status === 'active') throw storeError('ACCOUNT_ALREADY_ACTIVE', 'account already active');
            if (account.status !== 'pending_verification') throw storeError('EMAIL_NOT_VERIFIED', 'account not pending verification');

            const challengeRs = await client.query(
                `SELECT code_hash, expires_at, used_at, attempt_count
                 FROM email_verification_challenges
                 WHERE account_id = $1
                 LIMIT 1
                 FOR UPDATE`,
                [account.id]
            );
            if (!challengeRs.rowCount) throw storeError('VERIFICATION_CODE_EXPIRED', 'verification challenge not found');
            const challenge = challengeRs.rows[0];
            if (challenge.used_at) throw storeError('VERIFICATION_CODE_EXPIRED', 'verification challenge already used');
            if (toMs(challenge.expires_at) < now) throw storeError('VERIFICATION_CODE_EXPIRED', 'verification challenge expired');
            if (Number(challenge.attempt_count || 0) >= maxVerifyAttempts) {
                throw storeError('VERIFICATION_RATE_LIMITED', 'verification attempts exceeded');
            }

            const givenHash = hashOtp(otp);
            if (!timingSafeHexEqual(challenge.code_hash, givenHash)) {
                await client.query(
                    `UPDATE email_verification_challenges
                     SET attempt_count = attempt_count + 1, updated_at = NOW()
                     WHERE account_id = $1`,
                    [account.id]
                );
                await client.query('COMMIT');
                finalized = true;
                throw storeError('INVALID_VERIFICATION_CODE', 'invalid verification code');
            }

            await client.query(
                `UPDATE accounts
                 SET status = 'active', email_verified_at = NOW(), updated_at = NOW()
                 WHERE id = $1`,
                [account.id]
            );
            await client.query(
                `UPDATE email_verification_challenges
                 SET used_at = TO_TIMESTAMP($2), updated_at = NOW()
                 WHERE account_id = $1`,
                [account.id, nowSec]
            );
            const profileRs = await client.query(
                `SELECT profile_id FROM account_profile_links WHERE account_id = $1 LIMIT 1`,
                [account.id]
            );
            const profileId = profileRs.rowCount ? profileRs.rows[0].profile_id : null;
            await client.query('COMMIT');
            finalized = true;
            return {
                accountId: account.id,
                email: account.email,
                username: account.username,
                profileId
            };
        } catch (err) {
            if (!finalized) {
                try { await client.query('ROLLBACK'); } catch (_) {}
            }
            throw err;
        } finally {
            client.release();
        }
    }

    async findAccountByLogin(params) {
        const login = String(params && params.login ? params.login : '').trim();
        if (!login) return null;
        const emailNorm = normalizeEmail(login);
        const usernameNorm = normalizeUsername(login);
        const rs = await this.pool.query(
            `SELECT
                a.id AS account_id,
                a.email,
                a.username,
                a.password_hash,
                a.status,
                apl.profile_id,
                up.nickname AS profile_nickname,
                up.friend_code AS profile_friend_code
             FROM accounts a
             LEFT JOIN account_profile_links apl ON apl.account_id = a.id
             LEFT JOIN user_profiles up ON up.id = apl.profile_id
             WHERE a.email_norm = $1 OR a.username_norm = $2
             ORDER BY a.created_at ASC
             LIMIT 1`,
            [emailNorm, usernameNorm]
        );
        if (!rs.rowCount) return null;
        const row = rs.rows[0];
        return {
            accountId: row.account_id,
            email: row.email,
            username: row.username,
            passwordHash: row.password_hash,
            status: row.status,
            profileId: row.profile_id,
            profileNickname: row.profile_nickname,
            profileFriendCode: row.profile_friend_code
        };
    }

    async switchToGuestProfileForDevice(params) {
        const persistentId = String(params && params.persistentId ? params.persistentId : '').trim();
        const fallbackNickname = sanitizeNickname(params && params.nickname);
        if (!persistentId) throw storeError('MISSING_PERSISTENT_ID', 'missing persistentId');
        const client = await this.pool.connect();
        try {
            await client.query('BEGIN');
            await client.query(
                `INSERT INTO devices (persistent_id, last_seen_at)
                 VALUES ($1, NOW())
                 ON CONFLICT (persistent_id)
                 DO UPDATE SET last_seen_at = NOW()`,
                [persistentId]
            );

            const guestRs = await client.query(
                `SELECT up.id
                 FROM device_profiles dp
                 JOIN user_profiles up ON up.id = dp.profile_id
                 LEFT JOIN account_profile_links apl ON apl.profile_id = up.id
                 LEFT JOIN accounts a ON a.id = apl.account_id AND a.status = 'active'
                 WHERE dp.device_persistent_id = $1 AND a.id IS NULL
                 ORDER BY dp.updated_at DESC
                 LIMIT 1`,
                [persistentId]
            );

            let profileId = null;
            if (!guestRs.rowCount) {
                const created = await this.createProfileForDevice(client, persistentId, fallbackNickname);
                profileId = created.id;
            } else {
                profileId = guestRs.rows[0].id;
                await this.setActiveProfileForDeviceTx(client, persistentId, profileId);
            }

            const snapshot = await this.getProfileSnapshotById(profileId, client);
            await client.query('COMMIT');
            return snapshot;
        } catch (err) {
            try { await client.query('ROLLBACK'); } catch (_) {}
            throw err;
        } finally {
            client.release();
        }
    }

    async createPendingLinkedAccount(params) {
        const profileId = String(params && params.profileId ? params.profileId : '').trim();
        const email = String(params && params.email ? params.email : '').trim();
        const username = String(params && params.username ? params.username : '').trim();
        const passwordHash = String(params && params.passwordHash ? params.passwordHash : '').trim();
        const codeTtlMs = Number(params && params.codeTtlMs) || (10 * 60 * 1000);
        const emailNorm = normalizeEmail(email);
        const usernameNorm = normalizeUsername(username);
        if (!profileId) throw storeError('PROFILE_NOT_FOUND', 'profile not found');
        if (!emailNorm) throw storeError('INVALID_EMAIL', 'invalid email');
        if (!usernameNorm) throw storeError('INVALID_USERNAME', 'invalid username');
        if (!passwordHash) throw storeError('INVALID_PASSWORD_HASH', 'invalid password hash');

        const client = await this.pool.connect();
        try {
            await client.query('BEGIN');
            const profileExists = await client.query(`SELECT id FROM user_profiles WHERE id = $1 LIMIT 1`, [profileId]);
            if (!profileExists.rowCount) throw storeError('PROFILE_NOT_FOUND', 'profile not found');

            const profileLinked = await client.query(
                `SELECT account_id FROM account_profile_links WHERE profile_id = $1 LIMIT 1`,
                [profileId]
            );
            if (profileLinked.rowCount) throw storeError('PROFILE_ALREADY_LINKED', 'profile already linked');

            const emailExists = await client.query(`SELECT id FROM accounts WHERE email_norm = $1 LIMIT 1`, [emailNorm]);
            if (emailExists.rowCount) throw storeError('EMAIL_ALREADY_USED', 'email already used');

            const usernameExists = await client.query(`SELECT id FROM accounts WHERE username_norm = $1 LIMIT 1`, [usernameNorm]);
            if (usernameExists.rowCount) throw storeError('USERNAME_TAKEN', 'username already used');

            const accountId = randomUuid();
            await client.query(
                `INSERT INTO accounts
                    (id, email, email_norm, username, username_norm, password_hash, status, created_at, updated_at)
                 VALUES ($1, $2, $3, $4, $5, $6, 'pending_verification', NOW(), NOW())`,
                [accountId, email, emailNorm, username, usernameNorm, passwordHash]
            );
            await client.query(
                `INSERT INTO account_profile_links (account_id, profile_id, linked_at)
                 VALUES ($1, $2, NOW())`,
                [accountId, profileId]
            );

            const code = makeOtpCode();
            const challengeId = randomUuid();
            await client.query(
                `INSERT INTO email_verification_challenges
                    (id, account_id, code_hash, expires_at, used_at, attempt_count, last_sent_at, send_count, send_window_start, created_at, updated_at)
                 VALUES ($1, $2, $3, NOW() + ($4::TEXT || ' milliseconds')::INTERVAL, NULL, 0, NOW(), 1, NOW(), NOW(), NOW())`,
                [challengeId, accountId, hashOtp(code), Math.max(1000, Math.floor(codeTtlMs))]
            );
            const expiresAt = Date.now() + Math.max(1000, Math.floor(codeTtlMs));
            await client.query('COMMIT');
            return {
                accountId,
                profileId,
                email,
                username,
                verificationCode: code,
                expiresAt
            };
        } catch (err) {
            try { await client.query('ROLLBACK'); } catch (_) {}
            if (err && err.code === '23505') {
                const msg = String(err.detail || err.message || '');
                if (msg.includes('email_norm')) throw storeError('EMAIL_ALREADY_USED', 'email already used');
                if (msg.includes('username_norm')) throw storeError('USERNAME_TAKEN', 'username already used');
            }
            throw err;
        } finally {
            client.release();
        }
    }

    async resendVerification(params) {
        const emailNorm = normalizeEmail(params && params.email);
        const codeTtlMs = Number(params && params.codeTtlMs) || (10 * 60 * 1000);
        const resendCooldownMs = Number(params && params.resendCooldownMs) || (60 * 1000);
        const maxSendsPerHour = Number(params && params.maxSendsPerHour) || 5;
        if (!emailNorm) throw storeError('INVALID_EMAIL', 'invalid email');

        const client = await this.pool.connect();
        try {
            await client.query('BEGIN');
            const accountRs = await client.query(
                `SELECT id, email, status
                 FROM accounts
                 WHERE email_norm = $1
                 LIMIT 1
                 FOR UPDATE`,
                [emailNorm]
            );
            if (!accountRs.rowCount) throw storeError('ACCOUNT_NOT_FOUND', 'account not found');
            const account = accountRs.rows[0];
            if (account.status === 'active') throw storeError('ACCOUNT_ALREADY_ACTIVE', 'account already active');

            const challengeRs = await client.query(
                `SELECT id, last_sent_at, send_count, send_window_start
                 FROM email_verification_challenges
                 WHERE account_id = $1
                 LIMIT 1
                 FOR UPDATE`,
                [account.id]
            );
            const now = Date.now();
            let sendCount = 0;
            let sendWindowStart = now;
            if (challengeRs.rowCount) {
                const row = challengeRs.rows[0];
                const lastSentAtMs = toMs(row.last_sent_at);
                if (now - lastSentAtMs < resendCooldownMs) {
                    throw storeError('VERIFICATION_RATE_LIMITED', 'verification resend cooldown', {
                        retryAfterMs: resendCooldownMs - (now - lastSentAtMs)
                    });
                }
                const windowMs = toMs(row.send_window_start);
                sendWindowStart = windowMs;
                sendCount = Number(row.send_count || 0);
                if (now - windowMs >= (60 * 60 * 1000)) {
                    sendWindowStart = now;
                    sendCount = 0;
                }
                if (sendCount >= maxSendsPerHour) {
                    throw storeError('VERIFICATION_RATE_LIMITED', 'verification resend rate limited');
                }
            }

            const code = makeOtpCode();
            const challengeId = challengeRs.rowCount ? challengeRs.rows[0].id : randomUuid();
            const expiresAt = now + Math.max(1000, Math.floor(codeTtlMs));
            const nextSendCount = sendCount + 1;
            if (!challengeRs.rowCount) {
                await client.query(
                    `INSERT INTO email_verification_challenges
                        (id, account_id, code_hash, expires_at, used_at, attempt_count, last_sent_at, send_count, send_window_start, created_at, updated_at)
                     VALUES ($1, $2, $3, NOW() + ($4::TEXT || ' milliseconds')::INTERVAL, NULL, 0, NOW(), $5, NOW(), NOW(), NOW())`,
                    [challengeId, account.id, hashOtp(code), Math.max(1000, Math.floor(codeTtlMs)), nextSendCount]
                );
            } else {
                await client.query(
                    `UPDATE email_verification_challenges
                     SET code_hash = $2,
                         expires_at = NOW() + ($3::TEXT || ' milliseconds')::INTERVAL,
                         used_at = NULL,
                         attempt_count = 0,
                         last_sent_at = NOW(),
                         send_count = $4,
                         send_window_start = TO_TIMESTAMP($5 / 1000.0),
                         updated_at = NOW()
                     WHERE account_id = $1`,
                    [account.id, hashOtp(code), Math.max(1000, Math.floor(codeTtlMs)), nextSendCount, sendWindowStart]
                );
            }

            await client.query('COMMIT');
            return {
                accountId: account.id,
                email: account.email,
                verificationCode: code,
                expiresAt
            };
        } catch (err) {
            try { await client.query('ROLLBACK'); } catch (_) {}
            throw err;
        } finally {
            client.release();
        }
    }

    async createProfileForDevice(client, persistentId, nickname) {
        for (let attempt = 0; attempt < 20; attempt++) {
            const profileId = randomUuid();
            const friendCode = makeFriendCode();
            try {
                await client.query(
                    `INSERT INTO user_profiles (id, nickname, friend_code) VALUES ($1, $2, $3)`,
                    [profileId, nickname, friendCode]
                );
                await this.setActiveProfileForDeviceTx(client, persistentId, profileId);
                return {
                    id: profileId,
                    nickname,
                    friendCode
                };
            } catch (err) {
                if (err && err.code === '23505') continue;
                throw err;
            }
        }
        throw storeError('FRIEND_CODE_GENERATION_FAILED', 'unable to generate unique friend code');
    }

    async ensureGuestProfile(params) {
        const persistentId = String(params && params.persistentId ? params.persistentId : '').trim();
        if (!persistentId) throw storeError('MISSING_PERSISTENT_ID', 'missing persistentId');
        const nickname = sanitizeNickname(params && params.nickname);
        const client = await this.pool.connect();
        try {
            await client.query('BEGIN');
            await client.query(
                `INSERT INTO devices (persistent_id, last_seen_at)
                 VALUES ($1, NOW())
                 ON CONFLICT (persistent_id)
                 DO UPDATE SET last_seen_at = NOW()`,
                [persistentId]
            );

            const existing = await client.query(
                `SELECT profile_id
                 FROM device_profiles
                 WHERE device_persistent_id = $1 AND is_active = TRUE
                 ORDER BY updated_at DESC
                 LIMIT 1`,
                [persistentId]
            );

            let profileId = null;
            if (!existing.rowCount) {
                const created = await this.createProfileForDevice(client, persistentId, nickname);
                profileId = created.id;
            } else {
                profileId = existing.rows[0].profile_id;
                await client.query(
                    `UPDATE device_profiles
                     SET updated_at = NOW()
                     WHERE device_persistent_id = $1 AND profile_id = $2`,
                    [persistentId, profileId]
                );
                await client.query(
                    `UPDATE user_profiles
                     SET nickname = $2, updated_at = NOW()
                     WHERE id = $1`,
                    [profileId, nickname]
                );
            }

            const snapshot = await this.getProfileSnapshotById(profileId, client);
            await client.query('COMMIT');
            return snapshot;
        } catch (err) {
            try { await client.query('ROLLBACK'); } catch (_) {}
            throw err;
        } finally {
            client.release();
        }
    }

    async getFriendsState(params) {
        const profileId = String(params && params.profileId ? params.profileId : '').trim();
        if (!profileId) throw storeError('PROFILE_NOT_FOUND', 'profile not found');

        const meRs = await this.pool.query(
            `SELECT
                up.id,
                up.nickname,
                up.friend_code,
                a.username,
                a.status AS account_status
             FROM user_profiles up
             LEFT JOIN account_profile_links apl ON apl.profile_id = up.id
             LEFT JOIN accounts a ON a.id = apl.account_id
             WHERE up.id = $1
             LIMIT 1`,
            [profileId]
        );
        if (!meRs.rowCount) throw storeError('PROFILE_NOT_FOUND', 'profile not found');
        const me = meRs.rows[0];
        const mePub = toPublicProfile(
            { id: me.id, nickname: me.nickname, friendCode: me.friend_code },
            me.account_status ? { status: me.account_status, username: me.username } : null
        );

        const friendsRs = await this.pool.query(
            `SELECT
                f.id AS friendship_id,
                f.created_at,
                CASE WHEN f.profile_a = $1 THEN f.profile_b ELSE f.profile_a END AS friend_profile_id,
                up.nickname,
                up.friend_code,
                a.username
             FROM friendships f
             JOIN user_profiles up
                ON up.id = (CASE WHEN f.profile_a = $1 THEN f.profile_b ELSE f.profile_a END)
             LEFT JOIN account_profile_links apl ON apl.profile_id = up.id
             LEFT JOIN accounts a ON a.id = apl.account_id AND a.status = 'active'
             WHERE f.profile_a = $1 OR f.profile_b = $1
             ORDER BY f.created_at DESC`,
            [profileId]
        );

        const incomingRs = await this.pool.query(
            `SELECT
                fr.id AS request_id,
                fr.from_profile_id,
                fr.to_profile_id,
                fr.created_at,
                up.nickname,
                up.friend_code,
                a.username
             FROM friend_requests fr
             JOIN user_profiles up ON up.id = fr.from_profile_id
             LEFT JOIN account_profile_links apl ON apl.profile_id = up.id
             LEFT JOIN accounts a ON a.id = apl.account_id AND a.status = 'active'
             WHERE fr.to_profile_id = $1 AND fr.status = 'pending'
             ORDER BY fr.created_at DESC`,
            [profileId]
        );

        const outgoingRs = await this.pool.query(
            `SELECT
                fr.id AS request_id,
                fr.from_profile_id,
                fr.to_profile_id,
                fr.created_at,
                up.nickname,
                up.friend_code,
                a.username
             FROM friend_requests fr
             JOIN user_profiles up ON up.id = fr.to_profile_id
             LEFT JOIN account_profile_links apl ON apl.profile_id = up.id
             LEFT JOIN accounts a ON a.id = apl.account_id AND a.status = 'active'
             WHERE fr.from_profile_id = $1 AND fr.status = 'pending'
             ORDER BY fr.created_at DESC`,
            [profileId]
        );

        return {
            profileId: mePub.profileId,
            friends: friendsRs.rows.map((row) => ({
                profileId: row.friend_profile_id,
                nickname: row.nickname,
                username: row.username || null,
                friendCode: row.friend_code,
                isGuest: !row.username
            })),
            incoming: incomingRs.rows.map((row) => ({
                requestId: row.request_id,
                fromProfileId: row.from_profile_id,
                toProfileId: row.to_profile_id,
                nickname: row.nickname,
                username: row.username || null,
                friendCode: row.friend_code,
                createdAt: toMs(row.created_at)
            })),
            outgoing: outgoingRs.rows.map((row) => ({
                requestId: row.request_id,
                fromProfileId: row.from_profile_id,
                toProfileId: row.to_profile_id,
                nickname: row.nickname,
                username: row.username || null,
                friendCode: row.friend_code,
                createdAt: toMs(row.created_at)
            })),
            incomingCount: incomingRs.rowCount
        };
    }

    async searchFriendProfiles(params) {
        const profileId = String(params && params.profileId ? params.profileId : '').trim();
        const query = String(params && params.query ? params.query : '').trim();
        const limit = Math.max(1, Math.min(20, Number(params && params.limit) || 10));
        if (!profileId) throw storeError('PROFILE_NOT_FOUND', 'profile not found');
        if (!query) return { results: [] };

        const queryFriendCode = normalizeFriendCode(query);
        const queryUsername = normalizeUsername(query);
        const candidateRs = await this.pool.query(
            `SELECT
                up.id,
                up.nickname,
                up.friend_code,
                a.username
             FROM user_profiles up
             LEFT JOIN account_profile_links apl ON apl.profile_id = up.id
             LEFT JOIN accounts a ON a.id = apl.account_id AND a.status = 'active'
             WHERE up.friend_code = $1 OR a.username_norm = $2
             LIMIT $3`,
            [queryFriendCode, queryUsername, limit]
        );
        if (!candidateRs.rowCount) return { results: [] };

        const results = [];
        for (const row of candidateRs.rows) {
            const candidateId = row.id;
            const isSelf = candidateId === profileId;
            let alreadyFriends = false;
            let pendingOutgoing = false;
            let pendingIncoming = false;

            if (!isSelf) {
                const frs = await this.pool.query(
                    `SELECT id
                     FROM friendships
                     WHERE (profile_a = $1 AND profile_b = $2)
                        OR (profile_a = $2 AND profile_b = $1)
                     LIMIT 1`,
                    [profileId, candidateId]
                );
                alreadyFriends = frs.rowCount > 0;

                if (!alreadyFriends) {
                    const pendingRs = await this.pool.query(
                        `SELECT from_profile_id, to_profile_id
                         FROM friend_requests
                         WHERE status = 'pending'
                           AND (
                             (from_profile_id = $1 AND to_profile_id = $2)
                             OR
                             (from_profile_id = $2 AND to_profile_id = $1)
                           )
                         LIMIT 1`,
                        [profileId, candidateId]
                    );
                    if (pendingRs.rowCount) {
                        const pending = pendingRs.rows[0];
                        pendingOutgoing = pending.from_profile_id === profileId;
                        pendingIncoming = pending.from_profile_id === candidateId;
                    }
                }
            }

            results.push({
                profileId: candidateId,
                nickname: row.nickname,
                username: row.username || null,
                friendCode: row.friend_code,
                isGuest: !row.username,
                eligibility: {
                    isSelf,
                    alreadyFriends,
                    pendingOutgoing,
                    pendingIncoming,
                    canRequest: !(isSelf || alreadyFriends || pendingOutgoing || pendingIncoming)
                }
            });
        }
        return { results };
    }

    async sendFriendRequest(params) {
        const fromProfileId = String(params && params.fromProfileId ? params.fromProfileId : '').trim();
        const toProfileId = String(params && params.toProfileId ? params.toProfileId : '').trim();
        if (!fromProfileId || !toProfileId) throw storeError('PROFILE_NOT_FOUND', 'profile not found');
        if (fromProfileId === toProfileId) throw storeError('FRIEND_REQUEST_NOT_ALLOWED', 'cannot request self');

        const client = await this.pool.connect();
        try {
            await client.query('BEGIN');

            const profilesRs = await client.query(
                `SELECT id
                 FROM user_profiles
                 WHERE id = ANY($1::uuid[])`,
                [[fromProfileId, toProfileId]]
            );
            if (profilesRs.rowCount < 2) throw storeError('PROFILE_NOT_FOUND', 'profile not found');

            const friendsRs = await client.query(
                `SELECT id
                 FROM friendships
                 WHERE (profile_a = $1 AND profile_b = $2)
                    OR (profile_a = $2 AND profile_b = $1)
                 LIMIT 1`,
                [fromProfileId, toProfileId]
            );
            if (friendsRs.rowCount) throw storeError('ALREADY_FRIENDS', 'already friends');

            const pendingRs = await client.query(
                `SELECT id
                 FROM friend_requests
                 WHERE status = 'pending'
                   AND (
                     (from_profile_id = $1 AND to_profile_id = $2)
                     OR
                     (from_profile_id = $2 AND to_profile_id = $1)
                   )
                 LIMIT 1`,
                [fromProfileId, toProfileId]
            );
            if (pendingRs.rowCount) throw storeError('FRIEND_REQUEST_ALREADY_EXISTS', 'friend request already exists');

            const requestId = randomUuid();
            await client.query(
                `INSERT INTO friend_requests
                    (id, from_profile_id, to_profile_id, status, created_at, responded_at)
                 VALUES ($1, $2, $3, 'pending', NOW(), NULL)`,
                [requestId, fromProfileId, toProfileId]
            );
            await client.query('COMMIT');
            return {
                requestId,
                fromProfileId,
                toProfileId,
                status: 'pending'
            };
        } catch (err) {
            try { await client.query('ROLLBACK'); } catch (_) {}
            if (err && err.code === '23505') {
                throw storeError('FRIEND_REQUEST_ALREADY_EXISTS', 'friend request already exists');
            }
            throw err;
        } finally {
            client.release();
        }
    }

    async respondFriendRequest(params) {
        const profileId = String(params && params.profileId ? params.profileId : '').trim();
        const requestId = String(params && params.requestId ? params.requestId : '').trim();
        const accept = !!(params && params.accept);
        if (!profileId) throw storeError('PROFILE_NOT_FOUND', 'profile not found');
        if (!requestId) throw storeError('FRIEND_REQUEST_NOT_FOUND', 'friend request not found');

        const client = await this.pool.connect();
        try {
            await client.query('BEGIN');
            const reqRs = await client.query(
                `SELECT id, from_profile_id, to_profile_id, status
                 FROM friend_requests
                 WHERE id = $1
                 LIMIT 1
                 FOR UPDATE`,
                [requestId]
            );
            if (!reqRs.rowCount) throw storeError('FRIEND_REQUEST_NOT_FOUND', 'friend request not found');
            const req = reqRs.rows[0];
            if (req.to_profile_id !== profileId) throw storeError('FRIEND_REQUEST_NOT_ALLOWED', 'friend request not allowed');
            if (req.status !== 'pending') throw storeError('FRIEND_REQUEST_NOT_PENDING', 'friend request not pending');

            if (accept) {
                const existingRs = await client.query(
                    `SELECT id
                     FROM friendships
                     WHERE (profile_a = $1 AND profile_b = $2)
                        OR (profile_a = $2 AND profile_b = $1)
                     LIMIT 1`,
                    [req.from_profile_id, req.to_profile_id]
                );
                if (!existingRs.rowCount) {
                    await client.query(
                        `INSERT INTO friendships (id, profile_a, profile_b, created_at)
                         VALUES ($1, $2, $3, NOW())`,
                        [randomUuid(), req.from_profile_id, req.to_profile_id]
                    );
                }
            }

            await client.query(
                `UPDATE friend_requests
                 SET status = $2, responded_at = NOW()
                 WHERE id = $1`,
                [requestId, accept ? 'accepted' : 'rejected']
            );
            await client.query('COMMIT');
            return {
                requestId,
                fromProfileId: req.from_profile_id,
                toProfileId: req.to_profile_id,
                status: accept ? 'accepted' : 'rejected'
            };
        } catch (err) {
            try { await client.query('ROLLBACK'); } catch (_) {}
            if (err && err.code === '23505' && accept) {
                throw storeError('ALREADY_FRIENDS', 'already friends');
            }
            throw err;
        } finally {
            client.release();
        }
    }

    async close() {
        if (!this.pool) return true;
        await this.pool.end();
        this.pool = null;
        return true;
    }
}

function createIdentityStore() {
    const databaseUrl = String(process.env.DATABASE_URL || '').trim();
    if (!databaseUrl) return new MemoryIdentityStore();
    return new PostgresIdentityStore(databaseUrl);
}

module.exports = {
    createIdentityStore
};
