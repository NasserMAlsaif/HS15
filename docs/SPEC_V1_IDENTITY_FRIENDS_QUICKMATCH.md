# HeadShooter Spec v1: Identity, Accounts, Friends, Invites, Quick Match

Status: Draft for implementation
Date: 2026-02-16
Scope: Client (`public/index.html`) + Server (`server.js`) + persistent DB

## 1) Product goals

1. Let users play immediately as `Guest` with no signup friction.
2. Support mandatory `email verification` when creating a linked account.
3. Keep guest data safe. No automatic merge and no automatic deletion when signing in.
4. Add friends system with friend requests and notification badge.
5. Add party invites from any party member (Quick and Custom).
6. Add Quick Game queue flow with clear cancel confirmation.
7. Prepare architecture for future central matchmaking across multiple game servers.

## 2) Non-goals in v1

1. No cross-region latency optimization logic yet (single region launch).
2. No rank-based matchmaking in v1 (only basic fill).
3. No social graph recommendations in v1.

## 3) Core decisions (locked)

1. Internal `user_id` is hidden and never used for player-facing search.
2. Every gameplay profile starts as `Guest`.
3. A guest can do `Create Account and Link` (new account creation).
4. A user can also `Sign in` to an existing account without deleting the current guest profile.
5. Guest profile remains available after logout and is shown as `Not linked to account`.
6. `email verification` is required before account activation.
7. Public player name in matches is `nickname` (non-unique, editable anytime).
8. Account name is `username` (unique, searchable).
9. Add `friend_code` (short unique code) for search/support flows.
10. Friend request UI uses explicit confirmation modal and hides `+` when not applicable.

## 4) Identity model

## 4.1 Concepts

1. `User Profile`
   1. Owns progression (rank, skins, stats, friends).
   2. Can exist as guest or linked account profile.
2. `Account Credential`
   1. Email + username + password + verification status.
   2. Linked to exactly one user profile.
3. `Device Profile`
   1. A local device may store multiple profile pointers.
   2. Active profile can be switched by sign in / logout.

## 4.2 Mandatory behavior

1. First app open:
   1. Server creates guest `user_profile`.
   2. Player can play and add friends directly.
2. Create account and link:
   1. User enters `email + username + password`.
   2. Server creates pending account and sends OTP/link.
   3. Only after verification, account becomes active and linked to current profile.
3. Sign in to existing account:
   1. Switch active profile to account-linked profile.
   2. Current guest profile is preserved, not merged, not deleted.
4. Logout:
   1. Return to local guest/last local profile selector.
   2. If profile has no account, label it `Not linked to account`.

## 5) Naming and visibility policy

1. `nickname`
   1. Non-unique.
   2. Editable anytime.
   3. Displayed in match HUD, kill feed, results.
2. `username`
   1. Unique.
   2. Searchable by other users.
   3. Can be displayed in profile page as `@username`.
3. `friend_code`
   1. Short unique code (example: `HS8K2P`).
   2. Searchable and useful for support.
4. `user_id`
   1. Hidden from other users.
   2. Visible only internally and in logs/admin.

## 6) Data model (DB schema)

Use PostgreSQL for durable state.

### 6.1 Identity tables

1. `user_profiles`
   1. `id UUID PK`
   2. `nickname VARCHAR(32)`
   3. `friend_code VARCHAR(12) UNIQUE`
   4. `created_at TIMESTAMP`
   5. `updated_at TIMESTAMP`
2. `accounts`
   1. `id UUID PK`
   2. `email VARCHAR(320) UNIQUE`
   3. `username VARCHAR(32) UNIQUE`
   4. `password_hash TEXT`
   5. `status ENUM('pending_verification','active','suspended')`
   6. `email_verified_at TIMESTAMP NULL`
   7. `created_at TIMESTAMP`
3. `account_profile_links`
   1. `account_id UUID UNIQUE FK -> accounts.id`
   2. `profile_id UUID UNIQUE FK -> user_profiles.id`
   3. `linked_at TIMESTAMP`
4. `email_verification_challenges`
   1. `id UUID PK`
   2. `account_id UUID FK`
   3. `code_hash TEXT`
   4. `expires_at TIMESTAMP`
   5. `used_at TIMESTAMP NULL`
   6. `attempt_count INT`
   7. `last_sent_at TIMESTAMP`
   8. `send_count INT`
5. `devices`
   1. `id UUID PK`
   2. `persistent_id VARCHAR(128) UNIQUE`
   3. `last_seen_at TIMESTAMP`
6. `device_profiles`
   1. `device_id UUID FK`
   2. `profile_id UUID FK`
   3. `is_active BOOLEAN`
   4. `created_at TIMESTAMP`
7. `sessions`
   1. `id UUID PK`
   2. `profile_id UUID FK`
   3. `device_id UUID FK`
   4. `refresh_token_hash TEXT`
   5. `expires_at TIMESTAMP`
   6. `revoked_at TIMESTAMP NULL`

### 6.2 Social tables

1. `friend_requests`
   1. `id UUID PK`
   2. `from_profile_id UUID FK`
   3. `to_profile_id UUID FK`
   4. `status ENUM('pending','accepted','rejected','cancelled','expired')`
   5. `created_at TIMESTAMP`
   6. `responded_at TIMESTAMP NULL`
2. `friendships`
   1. `id UUID PK`
   2. `profile_a UUID FK`
   3. `profile_b UUID FK`
   4. `created_at TIMESTAMP`
   5. Unique unordered pair constraint.

### 6.3 Party and matchmaking tables

1. `parties`
   1. `id UUID PK`
   2. `leader_profile_id UUID FK`
   3. `mode ENUM('custom','quick')`
   4. `state ENUM('lobby','queueing','matched','in_game','disbanded')`
   5. `region VARCHAR(16)` default `me`
   6. `created_at TIMESTAMP`
2. `party_members`
   1. `party_id UUID FK`
   2. `profile_id UUID FK`
   3. `is_ready BOOLEAN`
   4. `joined_at TIMESTAMP`
3. `party_invites`
   1. `id UUID PK`
   2. `party_id UUID FK`
   3. `from_profile_id UUID FK`
   4. `to_profile_id UUID FK`
   5. `status ENUM('pending','accepted','rejected','expired','cancelled')`
   6. `expires_at TIMESTAMP`
4. `queue_tickets`
   1. `id UUID PK`
   2. `party_id UUID FK`
   3. `region VARCHAR(16)`
   4. `status ENUM('queued','matched','cancelled','expired')`
   5. `queued_at TIMESTAMP`
   6. `matched_at TIMESTAMP NULL`
   7. `cancel_reason VARCHAR(64) NULL`
   8. `cancelled_by_profile_id UUID NULL`
5. `game_servers`
   1. `id UUID PK`
   2. `region VARCHAR(16)`
   3. `status ENUM('healthy','degraded','draining','offline')`
   4. `current_matches INT`
   5. `max_matches INT`
   6. `last_heartbeat TIMESTAMP`

## 7) API contract (Auth/Account)

Use HTTP for auth and verification. Keep sockets for realtime game/social updates.

1. `POST /auth/signup-link`
   1. Input: `email`, `username`, `password`, `currentProfileToken`.
   2. Output: `verification_required`.
2. `POST /auth/verify-email`
   1. Input: `email`, `otp`.
   2. Output: `account_activated`, `session`.
3. `POST /auth/resend-verification`
   1. Input: `email`.
   2. Output: `resent=true` or cooldown error.
4. `POST /auth/signin`
   1. Input: `email_or_username`, `password`.
   2. Output: `session`, `profile`.
5. `POST /auth/logout`
   1. Input: `session`.
   2. Output: success.

## 8) Socket event contract (social, party, queue)

### 8.1 Friends

1. `friends:getList`
2. `friends:search`
   1. Query by exact `username` or exact `friend_code`.
3. `friends:sendRequest`
4. `friends:respondRequest`
5. `friends:incomingRequest` (server push with badge increment)
6. `friends:listUpdated` (server push)

### 8.2 Party invite

1. `party:inviteFriend`
   1. Allowed for any current party member.
   2. Works in `custom` and `quick`.
2. `party:inviteReceived`
3. `party:inviteRespond`
4. `party:memberUpdated`
5. `party:lobbyState`

### 8.3 Quick queue

1. `quick:startFindMatch` (leader only)
2. `quick:cancelFindMatch`
   1. leader cancel -> full ticket cancel.
   2. member cancel -> full ticket cancel, party stays lobby, member ready resets false.
3. `quick:queueStatus` (found/needed/elapsed)
4. `quick:matchFound` (includes secure join handoff token)
5. `quick:queueCancelled` (includes reason and actor)

## 9) UI requirements

## 9.1 Cancel queue confirmation

1. On cancel action show modal text:
   1. `Are you sure you want to cancel queue?`
2. Buttons:
   1. `Yes`
   2. `No`
3. Only on `Yes` emit `quick:cancelFindMatch`.

## 9.2 Friend request confirmation

1. In party list and results list, show `+` near player name when eligible.
2. On click `+`, show modal text:
   1. `Do you want to send a friend request to <player>?`
3. Buttons:
   1. `Yes`
   2. `No`
4. Do not show `+` when:
   1. target is self
   2. already friends
   3. pending outgoing request exists
   4. pending incoming request exists
   5. target not discoverable by policy

## 9.3 Account UI

1. Settings page section:
   1. Active profile type (`Guest` or `Account`)
   2. `nickname`
   3. `username` (if linked)
   4. `friend_code`
   5. label `Not linked to account` for guest profiles
2. Account actions:
   1. `Create account and link`
   2. `Sign in`
   3. `Logout`

## 10) Matchmaking architecture

## 10.1 v1 launch (single region, one game server)

1. Queue logic can run in same process for initial launch.
2. Region fixed to `me`.
3. Party-level tickets only.

## 10.2 v2 scale (multiple ME servers, central matchmaking)

1. Add `Redis` as shared queue/state.
2. Add `Matchmaker Worker` service:
   1. reads queue tickets
   2. groups parties
   3. allocates a healthy game server
3. Add server registry heartbeat.
4. Result:
   1. players connected through different ME gateways can still be matched together.

## 11) Security and anti-abuse

1. Password hashing: `Argon2id`.
2. Email verification mandatory before first sign in.
3. Verification controls:
   1. OTP expiry (example: 10 minutes)
   2. resend cooldown (example: 60 seconds)
   3. max resend per hour
   4. max verify attempts per challenge
4. Rate limit:
   1. friend search
   2. friend request send
   3. party invites
   4. queue start/cancel
5. Server-authoritative checks remain enforced for gameplay.

## 12) Logging and metrics

Collect these metrics from day one:

1. `queue_time_ms`
   1. `matched_at - queued_at`
2. `queue_cancel_rate`
   1. `cancelled_tickets / total_created_tickets`
3. `invite_accept_rate`
   1. `accepted_party_invites / sent_party_invites`
4. `friend_request_accept_rate`
5. `verification_success_rate`
6. `signup_to_verified_time_ms`
7. Cancel reasons breakdown:
   1. `leader_cancel`
   2. `member_cancel`
   3. `member_disconnect`
   4. `timeout`

## 13) Error codes (minimum set)

1. `EMAIL_ALREADY_USED`
2. `USERNAME_TAKEN`
3. `EMAIL_NOT_VERIFIED`
4. `INVALID_CREDENTIALS`
5. `VERIFICATION_CODE_EXPIRED`
6. `VERIFICATION_RATE_LIMITED`
7. `FRIEND_REQUEST_ALREADY_EXISTS`
8. `ALREADY_FRIENDS`
9. `PARTY_INVITE_NOT_ALLOWED`
10. `QUEUE_CANCEL_DENIED`
11. `QUEUE_NOT_FOUND`

## 14) Migration from current system

Current code uses `persistentId` and in-memory sessions.

1. Add DB with schema above.
2. On `registerPlayer`, map `persistentId` -> `devices`.
3. Bootstrap a `user_profile` for unknown device.
4. Keep current gameplay sockets working while reading identity from DB-backed session.
5. Move existing social/match state from memory to DB/Redis incrementally.

## 15) Acceptance criteria (must pass)

1. Guest first launch can play instantly.
2. Guest can send/receive friend requests.
3. `Create account and link` requires successful email verification before activation.
4. Sign in to existing account does not delete or merge current guest profile.
5. Logout returns to local guest profile with `Not linked to account` label.
6. `+` friend action in party/results follows visibility rules exactly.
7. Cancel queue always opens confirmation modal with exact copy and Yes/No behavior.
8. Any party member can send invite in both custom and quick parties.
9. Queue cancel metrics capture actor and reason.
10. With two ME game servers + central queue, players can match across servers.

## 16) Rollout phases

1. Phase A: DB + identity primitives + guest/account sessions.
2. Phase B: signup-link + verification + signin/logout.
3. Phase C: friends + requests + search (`username` and `friend_code`).
4. Phase D: party invites from any member.
5. Phase E: quick queue + cancel confirm + queue metrics.
6. Phase F: Redis queue + central worker for multi-server ME.

## 17) Rewarded Buff Rule (Locked)

1. Rewarded ad grants `Instant Respawn` buff.
2. Buff has up to `3 instant respawns` available in the next match.
3. If player enters a match and gets `0 deaths`, buff is not consumed and carries to next match.
4. If player gets at least `1 death` and instant respawn is used, buff is considered consumed for that match session.
5. At end of that match (where buff was used at least once), remove buff completely.
6. Player can watch a new rewarded ad to get buff again.
