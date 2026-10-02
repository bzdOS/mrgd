// START_AI_HEADER
// MODULE: matrix-hs/src/state.rs
// PURPOSE: Shared server state: a HashMap<room_id, RoomLog> behind a Mutex.
//          All handlers receive Arc<AppState> via axum's State extractor.
//
//          Stage 2 additions:
//            - server_name: String (from MATRIX_HS_SERVER_NAME env, default "localhost")
//            - public_base_url: Option<String> (from MATRIX_HS_PUBLIC_BASEURL env)
//            - room_state: per-room state events (separate from RoomLog timeline)
//            - aliases: #name:server → room_id mapping
//            - stream_pos: global monotonic counter for incremental sync
//            - notify: Notify for long-poll wakeup
//            - room_timeline: per-room (pos, event_json) list for incremental sync
//
//          Registration additions (Stage 2.5):
//            - users: local username → UserRecord map (plaintext pw skeleton; see UserRecord)
//            - uia_sessions: set of issued UIA session IDs (format "uia_<N>")
//            - uia_seq: AtomicU64 counter for session ID generation
//
//          Persistence additions (Stage 3):
//            - persist: PersistCtx (from persist.rs); enabled when MATRIX_HS_DATA_DIR is set.
//              Unset → pure in-memory (existing 13 tests unchanged).
//              Set → durable append-log per room + accounts + aliases journals.
//
//          `cluster` feature: AppState gains an optional ClusterState that holds
//          a per-room ZenohCrdtSink registry.  On PUT /send the handler publishes
//          a delta immediately; on GET /sync the handler drains incoming deltas
//          before assembling the timeline.  This makes multi-master convergence
//          observable at the HTTP level: POST to instance-A → GET on instance-B
//          sees the message after one Zenoh gossip round-trip (~200 ms loopback).
//
//          Implementation note: zenoh::Session is itself Arc-backed (Session(Arc<SessionInner>))
//          so cloning it is cheap — it just bumps an Arc refcount and shares the same
//          underlying Zenoh runtime.  Per-room ZenohCrdtSink instances each receive a
//          cloned session; they all share the same Zenoh network identity.
//
//          Alias uniqueness (coordination-free grow-set, mirrors username path):
//            - alias_provisional: set of full aliases provisionally registered pending
//              reconcile() on partition heal.  Managed by createRoom + ReconcileDriver.
//            - mark_alias_relinquished(): called by the ReconcileDriver loser handler
//              when this node holds the losing claimant for a grow-set alias conflict.
//              Removes the alias from the local map and flags it to stderr.
//              Full re-point (client notification, room alias update) is deferred —
//              same posture as the username rename flow ([see docs/DESIGN.md]).
//
//          Username rename (coordination-free loser rename, added Stage 3.1):
//            - renamed: Mutex<HashMap<orig_localpart, new_full_user_id>>
//              Populated by apply_username_loss() when the ReconcileDriver flags this
//              node as the loser.  Old tokens (tok_<orig>) resolve to the new user_id
//              via whoami/login so the client discovers the rename on next poll.
//              SCHEME: new_localpart = "<orig>--<loser_server>" where loser_server is
//              the Matrix homeserver part of the losing claimant MXID.  Pure function
//              of the losing claim → every node computes the same result identically
//              (coordination-free, no randomness, no clock).
//              RESIDUALS:
//                - No server-push: client discovers rename via GET /whoami on next poll.
//                - Already-sent events carry the old sender field; re-attribution is
//                  out of scope (same posture as [see docs/DESIGN.md]).
//                - The renamed map is node-local; cross-node propagation of the renamed
//                  map is not implemented (each node applies its own losses only).
//
//          OTK storage (Stage 3 — OWNERSHIP-PARTITION exactly-once claim barrier):
//            - device_otks: per-(user_id,device_id) map of "alg:key_id" → key JSON.
//              Keys are OWNED by the node they were uploaded to.  Claim pops atomically
//              (Mutex remove) — a given key is returned at most once (exactly-once barrier).
//              DEFERRED: cross-node claim routing to the owner via Zenoh queryable
//              (see routes/keys.rs — ownership-partition extension).
//            - device_keys: per-(user_id,device_id) stored device_keys JSON blob for query.
//
//          E2EE device-list change tracking (device-lists feature):
//            - device_list_changes: user_id → stream_pos of the most recent device-list
//              change (keys/upload device_keys, register, deactivate). See
//              AppState::mark_device_list_changed / device_list_changes_since /
//              users_sharing_room_with, and routes/keys.rs for the cross-node gossip.
//
//          To-device relay (real sendToDevice — replaces the old no-op stub):
//            - to_device_queue: per-(user_id,device_id) ordered Vec<(stream_pos, event)>.
//              stream_pos is drawn from the SAME counter as room sync (used for FIFO
//              ordering ONLY — the delivery ack is the per-device `delivered` watermark,
//              see drain_to_device; using the room since as the ack would silently drop
//              messages enqueued below a device's room cursor). See routes/to_device.rs
//              (PUT handler + cross-node gossip) and AppState::enqueue_to_device /
//              drain_to_device.
//            - delivered: per-(user_id,device_id) watermark of the highest stream_pos
//              actually returned by drain_to_device — the sole to-device delivery ack.
//            - to_device_seen: dedup set of msg_ids already enqueued (direct send + gossip
//              loop-back safety).
//
//          Media repository (routes/media.rs):
//            - media: media_id -> MediaEntry (content_type + bytes + owning node_id),
//              held in memory regardless of persistence mode so downloads are always
//              served from RAM once uploaded on THIS node. When MATRIX_HS_DATA_DIR is
//              set, store_media() ALSO fsyncs the blob to
//              <data_dir>/media/<sanitized_media_id> (+ a ".ct" sidecar carrying the
//              content-type) so a restart can restore it via persist::replay_media —
//              mirrors the room-journal / accounts.jsonl durability posture elsewhere
//              in this file.
//              owner_node records which node's upload created the entry (== server_name
//              at store time). This is NOT used for routing yet — see routes/media.rs's
//              module header for the CROSS-NODE FETCH SEAM (deferred, mirrors the
//              OWNERSHIP-PARTITION cross-node deferral pattern already used for OTK
//              claim in routes/keys.rs): a download miss on a node that did not receive
//              the upload currently 404s instead of querying the owner over Zenoh.
// DEPENDENCIES: std::sync::Mutex, crate::substrate::matrix_events::RoomLog,
//               persist::PersistCtx,
//               (cluster only) crate::substrate::crdt::ZenohCrdtSink, zenoh
// PUBLIC_API: AppState, StateEvent, UserRecord, ClusterConfig (cluster only)
// END_AI_HEADER

use crate::persist::PersistCtx;
use crate::substrate::barrier::ClaimStore;
use crate::substrate::matrix_events::RoomLog;
use crate::substrate::node_auth::{NodeKeyStore, NodeSigner};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    sync::{Arc, Mutex},
};

// ── Cluster-only imports ──────────────────────────────────────────────────────

#[cfg(feature = "cluster")]
use crate::substrate::crdt::ZenohCrdtSink;

type RoomKeyDataMap = HashMap<(String, String), HashMap<String, HashMap<String, Value>>>;
type ToDeviceQueue = HashMap<(String, String), Vec<(u64, Value)>>;
type TypingRemoteMap = HashMap<String, HashMap<String, HashMap<String, u64>>>;
type ReceiptsMap = HashMap<String, HashMap<String, (String, String, u64)>>;
type AccountDataRoomMap = HashMap<String, HashMap<String, HashMap<String, Value>>>;

// HLC_MAX_DRIFT_MS:start
//   purpose: How far ahead of local wall-clock time a peer's timestamp may be before
//            it is ignored rather than adopted. Five minutes is generous for genuine
//            clock skew between cooperating nodes and far short of the year-off values
//            a dead RTC produces.
//   input:  none
//   output: u64 milliseconds
//   sideEffects: none
// HLC_MAX_DRIFT_MS:end
pub const HLC_MAX_DRIFT_MS: u64 = 5 * 60 * 1000;

// ── StateEvent ────────────────────────────────────────────────────────────────

// StateEvent:start
//   purpose: A Matrix state event stored per-room, separate from the RoomLog timeline.
//            State events are returned in sync response's state.events array, not timeline.
//            They represent current room state (membership, power levels, name, etc.).
//   input:  constructed by createRoom and state update handlers
//   output: StateEvent value (serialisable to Matrix ClientEvent JSON)
//   sideEffects: none
// StateEvent:end
#[derive(Debug, Clone)]
pub struct StateEvent {
    pub event_type: String,
    pub state_key: String,
    pub sender: String,
    pub content: Value,
    pub event_id: String,
    pub room_id: String,
    pub origin_server_ts: u64,
}

// ── UserRecord ────────────────────────────────────────────────────────────────

// UserRecord:start
//   purpose: Per-user registration record stored in AppState.users (keyed by localpart).
//            password_hash stores an Argon2id PHC string (never plaintext).
//            device_id tracks the device created at registration.
//            provisional: true when the barrier returned Provisional{fence} (coordinator
//            unreachable, Policy::Optimistic) — the account is locally created but may
//            lose a reconcile() conflict on partition heal.  Reconcile-on-heal is modeled
//            by crate::substrate::barrier::reconcile() but is NOT yet driven by any heal path
//            (deferred milestone — see [see docs/DESIGN.md]).
//            rename_required: retained for backward compatibility with any callers that
//            inspect it; apply_username_loss() clears it to false after the rename is
//            applied (the account moves to the new localpart, so the flag on the NEW
//            record is always false).
//   input:  constructed by AppState::register_user()
//   output: UserRecord value
//   sideEffects: none
// UserRecord:end
// MediaEntry:start
//   purpose: One stored media blob (uploaded via POST /_matrix/media/*/upload) held in
//            AppState.media.media. Kept in memory unconditionally (fast download path); ALSO
//            durably persisted under <data_dir>/media/ when MATRIX_HS_DATA_DIR is set
//            (see AppState::store_media / persist::replay_media).
//   input:  constructed by AppState::store_media()
//   output: MediaEntry value
//   sideEffects: none
// MediaEntry:end
#[derive(Debug, Clone)]
pub struct MediaEntry {
    /// Original Content-Type header supplied at upload (e.g. "image/png").
    /// Falls back to "application/octet-stream" if the client omitted it.
    pub content_type: String,
    /// Raw file bytes exactly as uploaded.
    pub bytes: std::sync::Arc<Vec<u8>>,
    /// server_name of the node this media was uploaded to (== AppState.server_name at
    /// store time). Not yet used for routing — see routes/media.rs module header for the
    /// deferred cross-node fetch seam.
    pub owner_node: String,
}

#[derive(Debug, Clone)]
pub struct UserRecord {
    /// Argon2id PHC hash string (never plaintext). Use auth::verify_password() to check.
    pub password_hash: String,
    /// Device ID assigned at registration (e.g. "DEVICE1" or caller-supplied).
    pub device_id: String,
    /// True when this account was registered provisionally (barrier returned Provisional).
    /// The account is locally valid but subject to reconcile() on partition heal.
    /// Heal-driven reconcile is NOT yet implemented (deferred — see SPEC §8.6).
    pub provisional: bool,
    /// True when ReconcileDriver determined this account lost a grow-set uniqueness
    /// conflict (another node claimed the same username earlier).  Set then immediately
    /// cleared by apply_username_loss() after the rename is applied — the NEW record
    /// always has rename_required=false.  Retained for backward compatibility.
    pub rename_required: bool,
    /// Token revocation epoch (Phase 2 internal-task). Incremented on logout_devices, password change,
    /// or admin revocation. Tokens include this epoch at mint time; verify_token rejects any
    /// token whose epoch < current record.epoch (the token's epoch is stale). Immediately makes
    /// logout_devices real, password change end sessions, and admin session-kill possible.
    /// Default 0 for backward compatibility (newly registered users).
    pub epoch: u32,
}

// ── E2EE key backup (/room_keys) ─────────────────────────────────────────────

// RoomKeyBackupVersion:start
//   purpose: Per-(user_id, version) metadata for a Matrix key-backup version
//            (POST/GET/PUT/DELETE .../room_keys/version). algorithm/auth_data are
//            stored exactly as the client supplied them (opaque per spec — this
//            server never decrypts client E2EE payloads). etag is a monotonic
//            per-version counter bumped on every keys mutation (put/delete) that
//            touches this version's stored session-key data; serialised as a
//            decimal string in responses (Matrix etag is an opaque string).
//   input:  constructed by AppState::create_room_key_version
//   output: RoomKeyBackupVersion value
//   sideEffects: none
// RoomKeyBackupVersion:end
#[derive(Debug, Clone)]
pub struct RoomKeyBackupVersion {
    pub algorithm: Value,
    pub auth_data: Value,
    pub etag: u64,
}

// CrossSigningKeys:start
//   purpose: Per-user cross-signing key set uploaded via POST
//            /keys/device_signing/upload. Each field is the opaque JSON blob the
//            client supplied (a CrossSigningKey object per the Matrix spec: user_id,
//            usage, keys, signatures) — stored exactly as given, never validated or
//            re-derived. user_signing_key is PRIVATE (only returned to its owner by
//            keys/query); master_key/self_signing_key are PUBLIC (returned for any
//            queried user that has them).
//            Node-local only, in-memory — NOT persisted across restarts and NOT
//            cluster-replicated (see the module header note on cross_signing_keys
//            in AppState below for the single-node honesty statement).
//   input:  constructed by AppState::set_cross_signing_keys
//   output: CrossSigningKeys value
//   sideEffects: none
// CrossSigningKeys:end
#[derive(Debug, Clone, Default)]
pub struct CrossSigningKeys {
    pub master_key: Option<Value>,
    pub self_signing_key: Option<Value>,
    pub user_signing_key: Option<Value>,
}

// ── Push notifications (routes/pushers.rs, routes/push.rs) ──────────────────

// PusherRecord:start
//   purpose: One registered pusher (POST /pushers/set), keyed by (user_id, app_id,
//            pushkey) in AppState.push.pushers. Mirrors the Matrix Pusher object exactly;
//            `data` is stored opaquely (the client-supplied {"url":..., "format":...}
//            object) — this server never interprets `format`, it only reads
//            data.url as the Push Gateway endpoint to POST notifications to
//            (see routes/push.rs::dispatch_push).
//   input:  constructed by routes/pushers.rs::post_pushers_set
//   output: PusherRecord value
//   sideEffects: none
// PusherRecord:end
#[derive(Debug, Clone)]
pub struct PusherRecord {
    pub app_id: String,
    pub pushkey: String,
    pub kind: String,
    pub app_display_name: String,
    pub device_display_name: String,
    pub lang: String,
    /// Opaque client-supplied pusher data, e.g. {"url": "https://gw/_matrix/push/v1/notify", "format": "event_id_only"}.
    pub data: Value,
    /// Registration time (ms since epoch) — echoed back as devices[].pushkey_ts in
    /// outbound notify POSTs per the Push Gateway API.
    pub pushkey_ts: u64,
}

// ── ClusterConfig (cluster feature only) ─────────────────────────────────────

// ClusterConfig:start
//   purpose: Configuration for the Zenoh cluster layer.
//            Holds an already-open zenoh::Session, the key prefix used to scope
//            CRDT delta publications for this instance, and this node's server_name.
//            Pass to AppState::with_cluster() during server startup.
//            server_name is explicit here (rather than read from the process-global
//            MATRIX_HS_SERVER_NAME env var) because two cluster nodes running as
//            separate AppState instances IN THE SAME PROCESS (e.g. cluster_test.rs)
//            cannot both read a distinct value from one process-wide env var — each
//            caller must supply its own server_name directly.  server_name doubles as
//            this node's node_id for signing (NodeSigner::node_id == server_name).
//   input:  session — open zenoh::Session (Arc-backed internally; cheap to clone);
//           key_prefix — e.g. "mrgd/matrix/room" (room_id appended per room);
//           server_name — this node's Matrix server_name / signing node_id
//   output: ClusterConfig value
//   sideEffects: none (session kept alive while ClusterState lives)
// ClusterConfig:end
#[cfg(feature = "cluster")]
pub struct ClusterConfig {
    pub session: zenoh::Session,
    pub key_prefix: String,
    pub server_name: String,
}

// ── ClusterState (cluster feature only) ──────────────────────────────────────

// ClusterState:start
//   purpose: Per-process Zenoh cluster layer.
//            Holds a ZenohCrdtSink per room, keyed by room_id.
//            Sinks are created lazily the first time a room is touched while
//            cluster mode is active.  The underlying zenoh::Session is cloned
//            cheaply per sink (Session is Arc<SessionInner> internally).
//   input:  ClusterConfig at construction; room_id string at per-room access
//   output: &ZenohCrdtSink via sink_for() (async, creates lazily)
//   sideEffects: opens one Zenoh subscriber per new room (in ZenohCrdtSink::new);
//                spawns one background tokio task per room
// ClusterState:end
#[cfg(feature = "cluster")]
pub struct ClusterState {
    /// Master Zenoh session — cloned cheaply per room sink (Arc-backed).
    session: zenoh::Session,
    /// Key prefix: "mrgd/matrix/room" — room_id appended to derive per-room key.
    key_prefix: String,
    /// Per-room sinks — created lazily.
    sinks: Mutex<HashMap<String, Arc<ZenohCrdtSink>>>,
}

#[cfg(feature = "cluster")]
impl ClusterState {
    // ClusterState::new:start
    //   purpose: Build a ClusterState from a ClusterConfig.
    //   input:  cfg — ClusterConfig with open session + key_prefix
    //   output: ClusterState
    //   sideEffects: none (no network I/O here; session already open)
    // ClusterState::new:end
    pub fn new(cfg: ClusterConfig) -> Self {
        ClusterState {
            session: cfg.session,
            key_prefix: cfg.key_prefix,
            sinks: Mutex::new(HashMap::new()),
        }
    }

    // ClusterState::session:start
    //   purpose: Hand out the master Zenoh session (Arc-backed, so this is a refcount
    //            bump, not a new connection). main.rs declares the catch-up queryables
    //            on it.
    //
    //            It used to open a SIBLING session from the same env instead, which is
    //            a trap on any node that sets MATRIX_HS_ZENOH_LISTEN: the second
    //            session tries to bind the port the first one already holds, gets
    //            "Address already in use", and the whole catch-up subsystem is skipped.
    //            That silently disabled catch-up on exactly the node most likely to
    //            have it — the listener, i.e. the hub of an ssh -L / autossh topology,
    //            the one every spoke asks. Sharing the session cannot collide.
    //   input:  none
    //   output: a clone of the master session
    //   sideEffects: none
    // ClusterState::session:end
    pub fn session(&self) -> zenoh::Session {
        self.session.clone()
    }

    // ClusterState::key_prefix:start
    //   purpose: This cluster's key prefix (e.g. "mrgd/matrix/room") — needed
    //            wherever a caller derives a key itself (claim_key, media_key,
    //            etc.) rather than going through a method that already has it.
    //   input:  none
    //   output: &str
    //   sideEffects: none
    // ClusterState::key_prefix:end
    pub fn key_prefix(&self) -> &str {
        &self.key_prefix
    }

    // ClusterState::start_discovery:start
    //   purpose: Spawn a background task subscribing to `<key_prefix>/**` and, for every
    //            sample, ensure a per-room sink exists for the room_id encoded in the key
    //            and inject the payload into that sink's inbox.
    //
    //            This is room *discovery*: without it a node only ever drains rooms it
    //            already knows about locally, so a completely fresh — or previously
    //            partitioned — node never learns that a room was created on a peer. The
    //            wildcard subscriber closes that gap with no room-list gossip round.
    //
    //            Complements the per-room subscriber in `sink_for`: this one catches the
    //            *first* sample for an unknown room (which would otherwise be dropped,
    //            since the room's own subscriber does not exist yet); the per-room
    //            subscriber handles everything after.
    //   input:  self — must be an Arc so the spawned task can keep the state alive
    //   output: none
    //   sideEffects: spawns a tokio task; declares one Zenoh wildcard subscriber
    // ClusterState::start_discovery:end
    pub fn start_discovery(self: &Arc<Self>) {
        let self_arc = self.clone();
        let prefix = self.key_prefix.clone();
        tokio::spawn(async move {
            let sub_key = format!("{prefix}/**");
            let subscriber = match self_arc.session.declare_subscriber(&sub_key).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("[matrix-hs] discovery subscriber declare failed: {e}");
                    return;
                }
            };
            while let Ok(sample) = subscriber.recv_async().await {
                let full_key = sample.key_expr().as_str();
                // Expected shape: "<key_prefix>/<room_id>/<crdt_key>".
                let suffix = full_key
                    .strip_prefix(&prefix)
                    .and_then(|s| s.strip_prefix('/'))
                    .unwrap_or(full_key);
                if let Some(slash) = suffix.find('/') {
                    let room_id = &suffix[..slash];
                    let crdt_key = &suffix[slash + 1..];
                    let bytes = sample.payload().to_bytes().to_vec();
                    // Lazily create the per-room sink, then hand it the sample we just
                    // took delivery of — its own subscriber was declared too late to see it.
                    match self_arc.sink_for(room_id).await {
                        Ok(sink) => sink.inject(crdt_key, bytes),
                        Err(e) => eprintln!("[matrix-hs] discovery sink_for({room_id}): {e}"),
                    }
                }
            }
            // Subscriber closed (session dropped) — task exits cleanly.
        });
    }

    // ClusterState::list_room_ids:start
    //   purpose: Every room_id for which a per-room sink exists — including rooms this
    //            node learned about only through `start_discovery` and has no local
    //            entry for yet. `sync` unions this with the locally-known rooms so
    //            discovered rooms actually get drained.
    //   input:  none
    //   output: Vec<String>
    //   sideEffects: none
    // ClusterState::list_room_ids:end
    pub fn list_room_ids(&self) -> Vec<String> {
        match self.sinks.lock() {
            Ok(guard) => guard.keys().cloned().collect(),
            Err(_) => Vec::new(),
        }
    }

    // ClusterState::sink_for:start
    //   purpose: Return the ZenohCrdtSink for `room_id`, creating it lazily if absent.
    //            Creation clones the master session (cheap — Arc refcount bump),
    //            opens a Zenoh subscriber on `<key_prefix>/<room_id>/**`,
    //            and spawns a background task.  Subsequent calls return the cached sink.
    //   input:  room_id — string key identifying the room
    //   output: Result<Arc<ZenohCrdtSink>, String>
    //   sideEffects: may clone session, open a Zenoh subscriber, spawn a tokio task (first call)
    // ClusterState::sink_for:end
    pub async fn sink_for(&self, room_id: &str) -> Result<Arc<ZenohCrdtSink>, String> {
        // Fast path: sink already exists.
        {
            let guard = self.sinks.lock().map_err(|e| e.to_string())?;
            if let Some(sink) = guard.get(room_id) {
                return Ok(sink.clone());
            }
        }

        // Slow path: create a new sink for this room.
        let room_prefix = format!("{}/{}", self.key_prefix, room_id);

        // Clone the session — cheap because zenoh::Session is Arc<SessionInner>.
        let session_clone = self.session.clone();

        let sink = ZenohCrdtSink::new(session_clone, &room_prefix)
            .await
            .map_err(|e| format!("ZenohCrdtSink for room {room_id}: {e}"))?;

        let sink = Arc::new(sink);
        {
            let mut guard = self.sinks.lock().map_err(|e| e.to_string())?;
            guard
                .entry(room_id.to_string())
                .or_insert_with(|| sink.clone());
            Ok(guard[room_id].clone())
        }
    }

    // ClusterState::crdt_key:start
    //   purpose: Return the CRDT routing key used inside each room's sink prefix.
    //            A single fixed key "events" routes all PDU deltas for a room.
    //   input:  none
    //   output: &'static str "events"
    //   sideEffects: none
    // ClusterState::crdt_key:end
    pub fn crdt_key() -> &'static str {
        "events"
    }

    // ClusterState::state_crdt_key:start
    //   purpose: Return the CRDT routing key used inside each room's sink prefix for
    //            room STATE deltas (m.room.member, m.room.name, ...), separate from
    //            "events" (RoomLog/timeline PDUs) so state and timeline replication
    //            can be drained independently (see routes/room_state.rs
    //            publish_state_event / drain_cluster_state).
    //   input:  none
    //   output: &'static str "state"
    //   sideEffects: none
    // ClusterState::state_crdt_key:end
    pub fn state_crdt_key() -> &'static str {
        "state"
    }

    // ClusterState::room_from_key:start
    //   purpose: Pull the room_id out of a catch-up key "<prefix>/<room_id>/<leaf>".
    //            Used on both sides of catch-up: a queryable reads it to learn which
    //            room is being asked for, and a querier reads it off the REPLY key to
    //            learn which room a reply is about — which is how a node discovers
    //            rooms it had never heard of.
    //   input:  key — full key expression; prefix — the cluster key prefix;
    //           leaf — trailing segment, "history" or "state"
    //   output: Some(room_id) if the key has that exact shape, else None
    //   sideEffects: none
    // ClusterState::room_from_key:end
    pub fn room_from_key<'a>(key: &'a str, prefix: &str, leaf: &str) -> Option<&'a str> {
        let seg = key
            .strip_prefix(prefix)?
            .strip_prefix('/')?
            .strip_suffix(leaf)?
            .strip_suffix('/')?;
        // A room_id is one key chunk; anything with a '/' is a different shape.
        if seg.is_empty() || seg.contains('/') {
            None
        } else {
            Some(seg)
        }
    }

    // ClusterState::rooms_for_query:start
    //   purpose: Decide which rooms to answer a catch-up query for. A wildcard room
    //            segment means "every room this node has"; a concrete room_id means
    //            just that one. Answering wildcards from live state — rather than
    //            declaring one queryable per room at startup — is what lets a peer
    //            recover a room that was created AFTER this node started.
    //   input:  query_key — the queried key expression; prefix, leaf — as above;
    //           local — room ids this node can actually answer for
    //   output: the subset of `local` to reply for, in the given order
    //   sideEffects: none
    // ClusterState::rooms_for_query:end
    pub fn rooms_for_query(
        query_key: &str,
        prefix: &str,
        leaf: &str,
        local: Vec<String>,
    ) -> Vec<String> {
        match Self::room_from_key(query_key, prefix, leaf) {
            // Concrete room asked for: answer only if we have it.
            Some(rid) if rid != "*" && rid != "**" => {
                local.into_iter().filter(|r| r == rid).collect()
            }
            // Wildcard, or a key we cannot parse: answer for everything we have.
            // Being generous is safe — the reply key names the room, so a querier
            // can always tell what it received.
            _ => local,
        }
    }

    // ── Media ────────────────────────────────────────────────────────────────
    //
    // Media is pulled on demand, never gossiped. Blobs are megabytes; pushing
    // every upload to every node would flood the same Zenoh mesh that carries
    // room events, to replicate files most nodes will never be asked for. So a
    // node that is asked for media it does not have queries the mesh for it and
    // caches what comes back — the cost lands on the first reader, once.

    // ClusterState::media_key:start
    //   purpose: Key a media blob is served under: "<prefix>/media/<media_id>".
    //            The "media" segment keeps blobs out of the room key space, where
    //            "<prefix>/<room_id>/<leaf>" lives.
    //   input:  prefix — cluster key prefix; media_id
    //   output: the full key expression
    //   sideEffects: none
    // ClusterState::media_key:end
    pub fn media_key(prefix: &str, media_id: &str) -> String {
        format!("{prefix}/media/{media_id}")
    }

    // ClusterState::media_from_key:start
    //   purpose: Inverse of media_key — read the media_id out of a query or reply key.
    //   input:  key — full key expression; prefix — cluster key prefix
    //   output: Some(media_id) if the key has that exact shape, else None
    //   sideEffects: none
    // ClusterState::media_from_key:end
    pub fn media_from_key<'a>(key: &'a str, prefix: &str) -> Option<&'a str> {
        let id = key
            .strip_prefix(prefix)?
            .strip_prefix('/')?
            .strip_prefix("media")?
            .strip_prefix('/')?;
        if id.is_empty() || id.contains('/') {
            None
        } else {
            Some(id)
        }
    }

    // ClusterState::encode_media_reply:start
    //   purpose: Frame a media blob for the wire. Length-prefixed rather than JSON
    //            because the payload is raw bytes — base64 would inflate every
    //            transfer by a third for no benefit.
    //            Layout: u16be content_type len | content_type | u16be owner len |
    //            owner_node | blob (to end).
    //   input:  content_type; owner_node — the node that originally accepted the
    //           upload, carried so a cached copy does not claim local ownership;
    //           blob — raw bytes
    //   output: the framed payload
    //   sideEffects: none
    // ClusterState::encode_media_reply:end
    pub fn encode_media_reply(content_type: &str, owner_node: &str, blob: &[u8]) -> Vec<u8> {
        let ct = content_type.as_bytes();
        let ow = owner_node.as_bytes();
        // Truncation is not a risk worth branching on: both fields are a MIME type
        // and a server name, and u16 caps them at 64 KiB.
        let ct_len = ct.len().min(u16::MAX as usize);
        let ow_len = ow.len().min(u16::MAX as usize);
        let mut out = Vec::with_capacity(4 + ct_len + ow_len + blob.len());
        out.extend_from_slice(&(ct_len as u16).to_be_bytes());
        out.extend_from_slice(&ct[..ct_len]);
        out.extend_from_slice(&(ow_len as u16).to_be_bytes());
        out.extend_from_slice(&ow[..ow_len]);
        out.extend_from_slice(blob);
        out
    }

    // ClusterState::decode_media_reply:start
    //   purpose: Inverse of encode_media_reply. Rejects anything that does not parse
    //            exactly rather than guessing — this decodes bytes from the network.
    //   input:  bytes — a framed payload
    //   output: Some((content_type, owner_node, blob)) or None if malformed
    //   sideEffects: none
    // ClusterState::decode_media_reply:end
    pub fn decode_media_reply(bytes: &[u8]) -> Option<(String, String, Vec<u8>)> {
        let mut at = 0usize;
        let take_str = |buf: &[u8], at: &mut usize| -> Option<String> {
            if buf.len() < *at + 2 {
                return None;
            }
            let len = u16::from_be_bytes([buf[*at], buf[*at + 1]]) as usize;
            *at += 2;
            if buf.len() < *at + len {
                return None;
            }
            let s = std::str::from_utf8(&buf[*at..*at + len]).ok()?.to_string();
            *at += len;
            Some(s)
        };
        let content_type = take_str(bytes, &mut at)?;
        let owner_node = take_str(bytes, &mut at)?;
        Some((content_type, owner_node, bytes[at..].to_vec()))
    }

    // ClusterState::fetch_media:start
    //   purpose: Ask the mesh for a media blob this node does not have. The key is
    //            concrete, and a node that lacks the blob stays silent, so the only
    //            replies are from nodes that actually hold it — no owner-to-node
    //            mapping has to be gossiped to find it.
    //   input:  media_id; timeout — budget per receive; max_bytes — refuse a reply
    //           larger than this (a peer's payload is untrusted input)
    //   output: Some(MediaEntry) from the first reply that decodes, else None
    //   sideEffects: one Zenoh query; logs refusals and malformed replies
    // ClusterState::fetch_media:end
    pub async fn fetch_media(
        &self,
        media_id: &str,
        timeout: std::time::Duration,
        max_bytes: usize,
    ) -> Option<MediaEntry> {
        let key = Self::media_key(&self.key_prefix, media_id);
        let replies = match self.session.get(&key).timeout(timeout).await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("[matrix-hs] media fetch: GET {key}: {e}");
                return None;
            }
        };
        while let Ok(Ok(reply)) = tokio::time::timeout(timeout, replies.recv_async()).await {
            let sample = match reply.result() {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("[matrix-hs] media fetch {media_id}: reply error: {e}");
                    continue;
                }
            };
            let bytes = sample.payload().to_bytes();
            if bytes.is_empty() {
                continue;
            }
            if bytes.len() > max_bytes {
                eprintln!(
                    "[matrix-hs] media fetch {media_id}: reply of {} bytes exceeds the \
                     {max_bytes}-byte cap — refused",
                    bytes.len()
                );
                continue;
            }
            match Self::decode_media_reply(&bytes) {
                Some((content_type, owner_node, blob)) => {
                    return Some(MediaEntry {
                        content_type,
                        bytes: Arc::new(blob),
                        owner_node,
                    });
                }
                None => eprintln!(
                    "[matrix-hs] media fetch {media_id}: malformed reply ({} bytes) — skipped",
                    bytes.len()
                ),
            }
        }
        None
    }

    // ── OTK claims (cross-node) ─────────────────────────────────────────────
    //
    // A device's one-time-keys are owned by whichever node its owner uploaded
    // them to (never replicated — device_otks is node-local, per the agent handoff record (kept private)).
    // Mirrors the media pattern exactly: a concrete key names the (user,
    // device, algorithm) being claimed; a node without a match for it stays
    // silent, so the only replies are from the one node that could ever have
    // an answer. Segments are base64url-encoded because a raw user_id's
    // leading '@' does not survive Zenoh key-expression matching intact.

    // ClusterState::claim_key:start
    //   purpose: Key an OTK claim is served under:
    //            "<prefix>/keys/claim/<b64 user_id>/<b64 device_id>/<b64 algorithm>".
    //            Each segment base64url-encoded (URL_SAFE_NO_PAD, same engine
    //            persist.rs already uses) — a raw user_id's '@'/':' does not
    //            survive Zenoh's key-expression grammar as a literal.
    //   input:  prefix — cluster key prefix; user_id, device_id, algorithm
    //   output: the full key expression
    //   sideEffects: none
    // ClusterState::claim_key:end
    pub fn claim_key(prefix: &str, user_id: &str, device_id: &str, algorithm: &str) -> String {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
        format!(
            "{prefix}/keys/claim/{}/{}/{}",
            URL_SAFE_NO_PAD.encode(user_id),
            URL_SAFE_NO_PAD.encode(device_id),
            URL_SAFE_NO_PAD.encode(algorithm)
        )
    }

    // ClusterState::claim_from_key:start
    //   purpose: Inverse of claim_key — decode the (user_id, device_id, algorithm)
    //            out of a concrete query key. Malformed/undecodable segments
    //            yield None rather than a panic — this parses untrusted input
    //            off the wire.
    //   input:  key — full key expression; prefix — cluster key prefix
    //   output: Some((user_id, device_id, algorithm)) if the key has that exact
    //           shape and every segment decodes as UTF-8, else None
    //   sideEffects: none
    // ClusterState::claim_from_key:end
    pub fn claim_from_key(key: &str, prefix: &str) -> Option<(String, String, String)> {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
        let rest = key
            .strip_prefix(prefix)?
            .strip_prefix('/')?
            .strip_prefix("keys")?
            .strip_prefix('/')?
            .strip_prefix("claim")?
            .strip_prefix('/')?;
        let mut parts = rest.split('/');
        let (u, d, a) = (parts.next()?, parts.next()?, parts.next()?);
        if parts.next().is_some() {
            return None; // extra segments — not a well-formed claim key
        }
        let decode = |seg: &str| -> Option<String> {
            String::from_utf8(URL_SAFE_NO_PAD.decode(seg).ok()?).ok()
        };
        Some((decode(u)?, decode(d)?, decode(a)?))
    }

    // ClusterState::encode_claim_reply / decode_claim_reply:start
    //   purpose: Frame a claimed OTK for the wire. JSON, not the media blob's
    //            binary framing — the payload here is a short key id plus a
    //            small JSON key object, the same scale as the state queryable's
    //            own JSON replies, not megabytes.
    //   input:  encode: key_id, key_value — the claimed "alg:key_id" -> key JSON;
    //           decode: bytes — a framed reply
    //   output: encode: the framed payload; decode: Some((key_id, key_value)) or
    //           None if malformed
    //   sideEffects: none
    // ClusterState::encode_claim_reply / decode_claim_reply:end
    pub fn encode_claim_reply(key_id: &str, key_value: &serde_json::Value) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({ "key_id": key_id, "key_value": key_value }))
            .unwrap_or_default()
    }

    pub fn decode_claim_reply(bytes: &[u8]) -> Option<(String, serde_json::Value)> {
        let parsed: serde_json::Value = serde_json::from_slice(bytes).ok()?;
        let key_id = parsed.get("key_id")?.as_str()?.to_string();
        let key_value = parsed.get("key_value")?.clone();
        Some((key_id, key_value))
    }

    // ClusterState::fetch_otk:start
    //   purpose: Ask the mesh for an OTK this node does not own. The key is
    //            concrete (a specific user/device/algorithm), and a node that
    //            does not own that device's keys stays silent, so the first
    //            (and only, since a key exists on at most one node and is
    //            popped atomically there) reply is authoritative.
    //   input:  user_id, device_id, algorithm; timeout — budget per receive
    //   output: Some((key_id, key_value)) from the owning node's reply, or
    //           None if no node has it, the reply was malformed, or the
    //           query itself failed
    //   sideEffects: one Zenoh query; logs failures/malformed replies
    // ClusterState::fetch_otk:end
    pub async fn fetch_otk(
        &self,
        user_id: &str,
        device_id: &str,
        algorithm: &str,
        timeout: std::time::Duration,
    ) -> Option<(String, serde_json::Value)> {
        let key = Self::claim_key(&self.key_prefix, user_id, device_id, algorithm);
        let replies = match self.session.get(&key).timeout(timeout).await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("[matrix-hs] otk claim fetch: GET {key}: {e}");
                return None;
            }
        };
        while let Ok(Ok(reply)) = tokio::time::timeout(timeout, replies.recv_async()).await {
            let sample = match reply.result() {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("[matrix-hs] otk claim fetch {user_id}/{device_id}: reply error: {e}");
                    continue;
                }
            };
            let bytes = sample.payload().to_bytes();
            if bytes.is_empty() {
                continue;
            }
            match Self::decode_claim_reply(&bytes) {
                Some(pair) => return Some(pair),
                None => eprintln!(
                    "[matrix-hs] otk claim fetch {user_id}/{device_id}: malformed reply \
                     ({} bytes) — skipped",
                    bytes.len()
                ),
            }
        }
        None
    }
}

// ── Node signing (P1.1 internal-task: wire node→node signing into the live cluster path) ──

// build_signer:start
//   purpose: Construct this node's NodeSigner plus a NodeKeyStore pre-seeded with the
//            node's own pubkey (a node always TOFU-trusts its own key — inserted here so
//            self-signed PDUs verify locally without any network round-trip).
//            When data_dir is Some: the ed25519 seed is persisted to
//            <data_dir>/node_ed25519.key via NodeSigner::load_or_generate — survives
//            restarts.  On load/generate failure (e.g. unwritable dir), falls back to a
//            fresh RANDOM key via NodeSigner::generate() and logs why.
//            When data_dir is None (in-memory AppState variants — new()/with_server_name()/
//            tests): uses NodeSigner::generate() — a random unforgeable key, NOT persisted
//            across restarts (acceptable: these variants have no durable state to protect).
//            Never a key DERIVED from node_id: node_id is public, so a derived key would be
//            forgeable by anyone (see NodeSigner::generate).
//   input:  node_id — this node's stable id, == server_name; data_dir — optional
//           persistence directory (mirrors PersistCtx::new's data_dir)
//   output: (Arc<NodeSigner>, Arc<NodeKeyStore>) — key_store already contains node_id's
//           own pubkey
//   sideEffects: may create/read <data_dir>/node_ed25519.key; writes to stderr if
//                load_or_generate fails (then falls back, does not panic)
// build_signer:end
fn build_signer(node_id: &str, data_dir: Option<&Path>) -> (Arc<NodeSigner>, Arc<NodeKeyStore>) {
    let signer = match data_dir {
        Some(dir) => match NodeSigner::load_or_generate(dir, node_id.to_string()) {
            Ok(s) => s,
            Err(e) => {
                eprintln!(
                    "[matrix-hs] NodeSigner::load_or_generate({}) failed: {e} — \
                     falling back to a random ephemeral key for node_id={node_id:?}",
                    dir.display()
                );
                NodeSigner::generate(node_id.to_string())
            }
        },
        None => NodeSigner::generate(node_id.to_string()),
    };
    // Trust anchor (internal-task). With MATRIX_HS_NODE_KEYS set, the set of nodes is fixed
    // here and the unauthenticated announcement channel can no longer introduce one:
    // whoever announces first stops owning the node_id. Unset → TOFU, as before.
    let key_store = match std::env::var("MATRIX_HS_NODE_KEYS") {
        Ok(spec) if !spec.trim().is_empty() => {
            match crate::substrate::node_auth::parse_node_keys(&spec) {
                Ok(mut pinned) => {
                    let own = signer.verifying_key_bytes();
                    match pinned.get(node_id) {
                        Some(k) if k != &own => {
                            // Loud, because it means the operator pinned a key this
                            // node cannot sign with: nobody will accept our events.
                            eprintln!(
                                "[node_auth] anchor: MATRIX_HS_NODE_KEYS pins a DIFFERENT \
                                 key for our own node_id {node_id:?} than the one we sign \
                                 with. Peers will reject everything we send. Fix the list \
                                 or the key file."
                            );
                        }
                        _ => {
                            pinned.insert(node_id.to_string(), own);
                        }
                    }
                    println!(
                        "node auth: anchored to {} configured node key(s) (TOFU disabled)",
                        pinned.len()
                    );
                    NodeKeyStore::anchored(pinned)
                }
                Err(e) => {
                    // FAIL CLOSED. Falling back to TOFU here would quietly turn a
                    // typo into "trust anyone", which is the opposite of what setting
                    // this variable asked for. Trust only ourselves until it is fixed.
                    eprintln!(
                        "[node_auth] anchor: MATRIX_HS_NODE_KEYS is malformed ({e}). \
                         Trusting NO peer keys until it is corrected — cross-node events \
                         will be rejected. This is deliberate: a broken allow-list must \
                         not silently become no allow-list."
                    );
                    let mut only_us = std::collections::HashMap::new();
                    only_us.insert(node_id.to_string(), signer.verifying_key_bytes());
                    NodeKeyStore::anchored(only_us)
                }
            }
        }
        _ => {
            let store = NodeKeyStore::new();
            // Self-trust: insert our own pubkey so locally-created (self-signed) PDUs
            // verify without needing any network round-trip.
            store.insert(node_id, signer.verifying_key_bytes());
            store
        }
    };
    (Arc::new(signer), Arc::new(key_store))
}

// ── AppState ──────────────────────────────────────────────────────────────────

// AppState:start
//   purpose: Shared mutable state for all handlers.
//            rooms: per-room CRDT event log, keyed by room_id string (e.g. "!abc:localhost").
//            room_state: per-room state events (separate from RoomLog timeline).
//            aliases: #alias:server → room_id mapping.
//            alias_provisional: set of full aliases that were registered provisionally
//                   (barrier returned Provisional{fence} — coordinator unreachable, AP path).
//                   A provisional alias is locally valid but subject to reconcile() on heal.
//                   Heal-driven reconcile is NOT yet implemented (deferred — SPEC §8.6).
//            stream_pos: global monotonic counter for incremental sync pagination.
//            notify: Notify for long-poll wakeup on new events.
//            room_timeline: per-room (pos, event_json) list used for since-based sync.
//            server_name: Matrix server name (from env MATRIX_HS_SERVER_NAME, default "localhost").
//            public_base_url: optional public base URL override (from env MATRIX_HS_PUBLIC_BASEURL).
//            users: local user store — localpart → UserRecord (LOCAL uniqueness only;
//                   cluster-wide uniqueness requires a mrgd coordination barrier,
//                   see docs/specs/docs/DESIGN.md and AppState::register_user).
//            uia_sessions: set of active UIA session IDs (format "uia_<N>").
//            uia_seq: AtomicU64 counter used to mint UIA session IDs without Math/rand.
//            persist: PersistCtx — durable append-log journals; disabled when
//                   MATRIX_HS_DATA_DIR is unset (pure in-memory, all existing tests pass).
//            barrier_store: optional Arc<dyn ClaimStore + Send + Sync> for cluster-wide
//                   username AND alias uniqueness coordination.  None in single-node mode —
//                   the existing local-uniqueness path is used unchanged (all existing tests
//                   pass with None).  Injected via with_barrier_store() before wrapping
//                   in Arc, or populated in build_state's cluster branch.
//                   NOT feature-gated so it can be tested with MemClaimStore in the
//                   default build without enabling the cluster feature.
//            renamed: orig_localpart → new_full_user_id mapping.  Populated by
//                   apply_username_loss() when the ReconcileDriver loser handler fires.
//                   whoami and login resolve old tokens via this map so the client
//                   discovers its new identity on the next poll (no server-push).
//                   Node-local only — cross-node propagation is not implemented.
//            signer: this node's NodeSigner (node_id == server_name) — every locally
//                   created PDU is signed with it before insertion into a RoomLog
//                   (P1.1 internal-task).  key_store: TOFU grow-set of node_id → pubkey, seeded
//                   with signer's own pubkey; the verify path for network-received PDUs
//                   (routes/sync.rs drain, main.rs catch-up merge) uses this store.
//            Wrapped in Arc<Mutex<…>> so axum can clone Arc across tokio tasks.
//            cluster (optional, feature="cluster"): Zenoh-backed delta-sync layer.
//   input:  none (construct with AppState::new() or AppState::with_cluster())
//   output: AppState value (wrapped in Arc)
//   sideEffects: none
// AppState:end

// TurnConfig:start
//   purpose: Configuration for the VoIP TURN credential endpoint
//            (GET /_matrix/client/v3/voip/turnServer, see routes/voip.rs). Holds
//            the operator's TURN server URIs and the coturn `static-auth-secret`
//            used to mint short-lived (ephemeral) TURN credentials per the
//            standard TURN REST credential scheme (username = "<expiry>:<mxid>",
//            password = base64(HMAC-SHA1(secret, username))) — the same scheme
//            Synapse's turn_shared_secret uses, so it works against a stock
//            coturn configured with `use-auth-secret` + a matching
//            `static-auth-secret`. The homeserver never relays media; it only
//            hands the client these credentials so the client can reach the
//            (external) TURN server for NAT traversal.
//   input:  TurnConfig::from_env() reads MATRIX_HS_TURN_URIS (comma-separated),
//           MATRIX_HS_TURN_SHARED_SECRET, MATRIX_HS_TURN_TTL (seconds, default
//           86400). Returns None unless BOTH uris and secret are set — so an
//           unconfigured server keeps returning an empty {} turnServer response
//           (no VoIP), unchanged from the previous stub.
//   output: Option<TurnConfig>
//   sideEffects: reads environment variables (from_env only)
// TurnConfig:end
#[derive(Clone)]
pub struct TurnConfig {
    pub uris: Vec<String>,
    pub shared_secret: String,
    pub ttl_secs: u64,
}

impl TurnConfig {
    pub fn from_env() -> Option<TurnConfig> {
        let uris: Vec<String> = std::env::var("MATRIX_HS_TURN_URIS")
            .ok()?
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        let shared_secret = std::env::var("MATRIX_HS_TURN_SHARED_SECRET").ok()?;
        if uris.is_empty() || shared_secret.is_empty() {
            return None;
        }
        let ttl_secs = std::env::var("MATRIX_HS_TURN_TTL")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(86_400);
        Some(TurnConfig {
            uris,
            shared_secret,
            ttl_secs,
        })
    }
}

// E2ee:start
//   purpose: E2EE key management + durability, grouped into one sub-struct so
//            every field a fresh feature adds to "encryption" lands here, not
//            as another flat AppState field. All node-local / single-node-
//            correct (see each field's own doc comment for the specific
//            cross-node deferral it inherits from — device_otks/device_keys
//            OWNERSHIP-PARTITION, room_key_*/cross_signing_*/cross_signatures
//            not cluster-replicated).
//   input:  Default::default() — every field starts empty
//   output: E2ee value, held as AppState.e2ee
//   sideEffects: none (plain data holder — no impl block)
// E2ee:end
#[derive(Default)]
pub struct E2ee {
    /// Per-device one-time keys: (user_id, device_id) → HashMap<"alg:key_id", key JSON>.
    /// OWNERSHIP-PARTITION: keys are owned by the node they were uploaded to.
    /// Claim (keys/claim) pops one matching key atomically — never returned twice
    /// = exactly-once barrier. DEFERRED: cross-node owner-routing via Zenoh queryable
    /// (see routes/keys.rs).
    pub device_otks: Mutex<HashMap<(String, String), HashMap<String, serde_json::Value>>>,

    /// Per-device device_keys JSON blob uploaded via keys/upload (for keys/query).
    /// (user_id, device_id) → device_keys JSON value.
    pub device_keys: Mutex<HashMap<(String, String), serde_json::Value>>,

    /// Cross-signing keys (cross-signing feature): user_id -> CrossSigningKeys.
    /// SINGLE-NODE / NODE-LOCAL ONLY — mirrors device_keys/device_otks above.
    pub cross_signing_keys: Mutex<HashMap<String, CrossSigningKeys>>,

    /// Cross-signature blobs uploaded via POST /keys/signatures/upload, stored
    /// opaquely and keyed by (uploader_user_id, target_key_or_device_id) -> the
    /// raw JSON blob. Not merged into device_keys/cross_signing_keys — see
    /// routes/keys.rs post_signatures_upload doc comment. Node-local only.
    pub cross_signatures: Mutex<HashMap<(String, String), Value>>,

    /// E2EE key backup (/room_keys) — per-(user_id, version) backup metadata.
    /// version strings are minted by room_key_backup_seq (monotonically
    /// increasing per user, never reused even after delete). Persisted when
    /// MATRIX_HS_DATA_DIR is set via persist_room_key_version_op /
    /// replay_room_key_versions. Node-local: cross-node replication would
    /// require routing writes to whichever node the user is currently talking
    /// to (same shape of problem as the OTK OWNERSHIP-PARTITION deferral) —
    /// NOT implemented; single-node-correct only.
    pub room_key_versions: Mutex<HashMap<(String, String), RoomKeyBackupVersion>>,

    /// Per-user monotonic counter for minting the next backup version number
    /// ("1", "2", ...). Never decreases, never reuses a number after a delete.
    pub room_key_backup_seq: Mutex<HashMap<String, u64>>,

    /// Per-user "current" backup version — the version GET .../room_keys/version
    /// (no version in the path) resolves to. Cleared if that same version is
    /// later deleted (a deleted version is never silently re-adopted as current).
    pub room_key_current_version: Mutex<HashMap<String, String>>,

    /// Stored encrypted session-key backups: (user_id, version) -> room_id ->
    /// session_id -> KeyBackupData JSON blob (opaque per Matrix spec). Persisted
    /// via persist_room_key_data_op / replay when MATRIX_HS_DATA_DIR is set.
    pub room_key_data: Mutex<RoomKeyDataMap>,

    /// E2EE device-list change tracking: user_id → the stream_pos at which that
    /// user's device list most recently changed (keys/upload with device_keys
    /// present, register, or deactivate). Drawn from the SAME global stream_pos
    /// counter as room sync / to-device, so one since-token space covers all
    /// three — see mark_device_list_changed and device_list_changes_since.
    /// Node-local; cross-node reach is via gossip (see
    /// routes/keys.rs::gossip_device_list_change / drain_device_list_gossip).
    pub device_list_changes: Mutex<HashMap<String, u64>>,
}

// ToDevice:start
//   purpose: The sendToDevice relay's per-target-device queue + cross-node
//            gossip dedup, grouped together since they're always touched as a
//            pair (enqueue_to_device writes both; drain_to_device reads the
//            queue).
//   input:  Default::default()
//   output: ToDevice value, held as AppState.to_device
//   sideEffects: none (plain data holder)
// ToDevice:end
#[derive(Default)]
pub struct ToDevice {
    /// Per-target-device to-device message queue. Keyed by (target_user_id,
    /// target_device_id) → Vec<(stream_pos, event_json)> where event_json =
    /// {"sender", "type", "content"}. stream_pos is drawn from the SAME global
    /// counter used for room sync (AppState.stream_pos) — the position is used
    /// ONLY for FIFO ordering here, never as a delivery ack (see delivered).
    /// Delivery: enqueue_to_device() appends here; sync.rs/sliding_sync.rs call
    /// drain_to_device() on every GET, which returns pending events and advances
    /// the per-device delivered watermark (see delivered).
    pub to_device_queue: Mutex<ToDeviceQueue>,

    /// Per-(user,device) HIGHEST stream_pos actually returned to that device by
    /// drain_to_device(). This is the ONLY delivery ack for to-device messages.
    /// It must NOT be derived from the room-sync since token: to-device and room
    /// events share one global stream_pos counter, so on a busy node a freshly
    /// enqueued to-device message's pos can sit BELOW a device's room-sync
    /// cursor — using the since as the ack would silently GC the message on
    /// first read and it would never be delivered (the room_key-loss bug, see
    /// drain_to_device doc). The watermark is in-memory only (best-effort,
    /// at-least-once delivery, mirroring to_device_seen) and resets to zero on
    /// restart, which only ever re-delivers, never drops.
    pub delivered: Mutex<HashMap<(String, String), u64>>,

    /// Dedup set of to-device msg_ids already enqueued locally. A to-device
    /// message sent on one node is (a) enqueued directly AND (b) broadcast via
    /// Zenoh gossip, so a node may see the same message twice — this set makes
    /// enqueue_to_device() a no-op on replay (mirrors RoomLog's grow-only
    /// dedup-by-event_id idempotency).
    pub to_device_seen: Mutex<HashSet<String>>,
}

// Ephemeral:start
//   purpose: Ephemeral EDUs (typing / receipts / read markers) — transient,
//            never persisted to disk, always cluster-gossiped where noted.
//            Grouped together since routes/ephemeral.rs and the sync/
//            sliding_sync ephemeral-block builders always touch several of
//            these fields in the same call.
//   input:  Default::default()
//   output: Ephemeral value, held as AppState.ephemeral
//   sideEffects: none (plain data holder)
// Ephemeral:end
#[derive(Default)]
pub struct Ephemeral {
    /// room_id -> user_id -> expiry stream_pos-ish timestamp for LOCALLY-set
    /// typing indicators. See the design note above AppState::set_typing for
    /// the full lazy-expiry contract.
    pub typing: Mutex<HashMap<String, HashMap<String, u64>>>,
    /// room_id -> node_id -> (user_id -> expiry) — the last full typing
    /// snapshot published by EACH remote node (cluster feature).
    pub typing_remote: Mutex<TypingRemoteMap>,
    /// room_id -> user_id -> (receipt_type, event_id, ts) — last-writer-wins by
    /// timestamp so local and cluster-drained writes merge safely.
    pub receipts: Mutex<ReceiptsMap>,
    /// (room_id, user_id) -> event_id for the m.fully_read marker. Node-local
    /// only — NOT cluster-replicated.
    pub fully_read: Mutex<HashMap<(String, String), String>>,
}

// MediaStore:start
//   purpose: The media repository's in-memory blob index. Its own sub-struct
//            mainly for naming symmetry with the other feature groups — media
//            has just one field today, but this is where a future addition
//            (e.g. a thumbnail cache) would land.
//   input:  Default::default()
//   output: MediaStore value, held as AppState.media.media
//   sideEffects: none (plain data holder)
// MediaStore:end
#[derive(Default)]
pub struct MediaStore {
    /// media_id -> MediaEntry. In-memory index/cache, always populated
    /// regardless of persistence mode; see MediaEntry doc comment and
    /// routes/media.rs. A media_id uploaded on node A is stored ONLY in node
    /// A's copy of this map — cross-node fetch is a documented, tested,
    /// deferred seam (see routes/media.rs module header).
    pub media: Mutex<HashMap<String, MediaEntry>>,
}

// PushState:start
//   purpose: Registered push-notification pushers. Named PushState (not Push)
//            to avoid reading oddly at call sites (state.push.pushers).
//   input:  Default::default()
//   output: PushState value, held as AppState.push
//   sideEffects: none (plain data holder)
// PushState:end
#[derive(Default)]
pub struct PushState {
    /// (user_id, app_id, pushkey) -> PusherRecord. Populated/updated by POST
    /// /pushers/set (kind:null deletes the entry); read by GET /pushers and by
    /// routes/push.rs::dispatch_push to decide who/where to POST message
    /// notifications. Node-local only — NOT persisted, NOT cluster-replicated.
    pub pushers: Mutex<HashMap<(String, String, String), PusherRecord>>,
}

// AccountData:start
//   purpose: Account data + room tags (routes/account_data.rs) — client
//            settings storage, grouped since all three maps share the same
//            node-local, non-persisted, non-replicated posture and are always
//            read together when building a /sync response's account_data
//            blocks.
//   input:  Default::default()
//   output: AccountData value, held as AppState.account_data
//   sideEffects: none (plain data holder)
// AccountData:end
#[derive(Default)]
pub struct AccountData {
    /// Global per-user account data: user_id -> event_type -> opaque JSON
    /// content. Set via PUT /user/{userId}/account_data/{type}; surfaced in
    /// /sync's top-level account_data.events.
    pub account_data_global: Mutex<HashMap<String, HashMap<String, Value>>>,
    /// Per-room per-user account data: user_id -> room_id -> event_type ->
    /// opaque JSON content. Surfaced in each joined room's account_data.events.
    pub account_data_room: Mutex<AccountDataRoomMap>,
    /// Room tags (m.tag): user_id -> room_id -> tag -> opaque JSON content
    /// (typically {"order": <f64>}). Surfaced as a synthetic "m.tag"
    /// account_data event in that room's account_data.events block.
    pub room_tags: Mutex<AccountDataRoomMap>,
}

// AppServiceConfigType:start
//   purpose: Application-service identity for the agent socket. `token` is the
//            shared secret presented as a Bearer on AS calls (constant-time
//            compared); `prefix` is the exclusive localpart namespace this AS
//            owns — it may create and mint tokens ONLY for localparts starting
//            with the prefix, and nothing else (UIA registration, password
//            login) is granted by it.
//   input:   constructed from env or tests
//   output:  none (data)
//   sideEffects: none
// AppServiceConfigType:end
#[derive(Clone, Debug)]
pub struct AppServiceConfig {
    pub token: String,
    pub prefix: String,
}

impl AppServiceConfig {
    // AppServiceConfig::from_env:start
    //   purpose: Read MATRIX_HS_AS_TOKEN + MATRIX_HS_AS_PREFIX. Token without
    //            prefix → prefix defaults to "as_" (documented); prefix
    //            without token → ignored (no AS). One env pair, one service —
    //            the single-tenant shape Case 2 actually needs; plural AS
    //            support is not built until a second consumer asks.
    //   input:  process env
    //   output: Option<AppServiceConfig>
    //   sideEffects: none
    // AppServiceConfig::from_env:end
    pub fn from_env() -> Option<Self> {
        let token = std::env::var("MATRIX_HS_AS_TOKEN").ok()?;
        if token.trim().is_empty() {
            return None;
        }
        let prefix = std::env::var("MATRIX_HS_AS_PREFIX")
            .ok()
            .filter(|p| !p.trim().is_empty())
            .unwrap_or_else(|| "as_".to_string());
        Some(Self { token, prefix })
    }

    // AppServiceConfig::owns_localpart:start
    //   purpose: Namespace check — is this localpart inside the AS's exclusive
    //            prefix? The empty prefix owns nothing (defensive; from_env
    //            never produces one).
    //   input:  localpart (bare, no @ or :; localpart() normalises callers)
    //   output: bool
    //   sideEffects: none
    // AppServiceConfig::owns_localpart:end
    pub fn owns_localpart(&self, localpart: &str) -> bool {
        !self.prefix.is_empty() && localpart.starts_with(&self.prefix)
    }
}

pub struct AppState {
    /// Per-room CRDT event log.
    pub rooms: Mutex<HashMap<String, RoomLog>>,

    /// Per-room state events (m.room.create, m.room.member, etc.).
    /// Separate from RoomLog timeline — these go in state.events in sync, not timeline.events.
    pub room_state: Mutex<HashMap<String, Vec<StateEvent>>>,

    /// Room alias → room_id mapping. "#name:server" → "!id:server"
    pub aliases: Mutex<HashMap<String, String>>,

    /// Full aliases registered provisionally (barrier returned Provisional{fence}).
    /// Locally valid but subject to reconcile() on partition heal.
    /// Heal-driven reconcile is NOT yet implemented (deferred — SPEC §8.6).
    pub alias_provisional: Mutex<HashSet<String>>,

    /// Global monotonic stream position counter for incremental sync (s<N> tokens).
    pub stream_pos: Arc<AtomicU64>,

    /// Hybrid logical clock, in milliseconds, used as `origin_server_ts` for state
    /// events and therefore as the LWW ordering key. Seeded from the wall clock and
    /// pushed past anything seen from a peer, so ordering is causally coupled across
    /// nodes instead of depending on whose clock is fastest.
    pub hlc: Arc<AtomicU64>,

    /// Wakeup for long-poll /sync
    pub notify: Arc<tokio::sync::Notify>,

    /// Per-room timeline entries: (stream_pos, client_event_json).
    /// Message events and state events both land here so since-based sync works.
    /// Bounded by `timeline_max_events` (0 = unlimited); trimmed oldest-first
    /// via AppState::append_room_timeline so steady-state memory is capped.
    pub room_timeline: Mutex<HashMap<String, Vec<(u64, Value)>>>,

    /// Retention cap for each room's timeline (Phase 1 GC). 0 = unlimited
    /// (preserves the original append-only behaviour and all existing tests).
    /// When > 0, append_room_timeline trims the oldest entries per room to
    /// this size, and classic /sync signals `limited:true` + `prev_batch` so
    /// clients can backfill the dropped tail via /rooms/{id}/messages.
    pub timeline_max_events: usize,

    /// GC cap on the RoomLog itself — the CRDT set, not the read projection.
    /// 0 (default) = unlimited, the original behaviour. When exceeded,
    /// AppState::collect_room_log raises the log's depth watermark, which both
    /// drops the old events here and stops peers putting them back.
    /// This is the bound that actually matters: `timeline_max_events` caps what
    /// /sync can see, while the RoomLog is what grows forever underneath it.
    pub roomlog_max_events: usize,

    /// Redaction records, keyed by the REDACTED (target) event_id → the full
    /// client-event JSON of the m.room.redaction event that redacted it.
    /// NOT persisted as its own file: rebuilt on replay from the redaction events
    /// in the room journals, and populated on every replication path too — see
    /// routes/redact.rs's module header for the full list of writers.
    /// Populated by routes/redact.rs::put_redact_event. Consulted at read time
    /// by AppState::apply_redaction — every place that serves a timeline event
    /// (classic /sync build_join_rooms, /rooms/{id}/messages, sliding-sync
    /// build_rooms) masks content to {} and adds unsigned.redacted_because for
    /// any event_id present here. The underlying room_timeline entry is left
    /// UNTOUCHED (masking is read-side only) — this keeps redaction idempotent
    /// and avoids racing writers that already hold a room_timeline lock.
    pub redactions: Mutex<HashMap<String, Value>>,

    /// Matrix server name (e.g. "localhost").
    pub server_name: String,

    /// Optional public base URL override (e.g. "https://matrix.example.com").
    pub public_base_url: Option<String>,

    /// Registered users: localpart → UserRecord.
    /// LOCAL uniqueness only — see register_user() for the cross-node barrier note.
    pub users: Mutex<HashMap<String, UserRecord>>,

    /// Active UIA session IDs issued by POST /register (first-call 401 challenge).
    pub uia_sessions: Mutex<HashSet<String>>,

    /// Monotonic counter for minting UIA session IDs ("uia_<N>").
    pub uia_seq: AtomicU64,

    /// Durable persistence context.
    /// When MATRIX_HS_DATA_DIR is set: writes append-only journals + enables replay.
    /// When unset: all persist_* calls are no-ops (pure in-memory, existing tests unchanged).
    pub persist: PersistCtx,

    /// Optional barrier store for cluster-wide username and alias uniqueness coordination.
    /// None → single-node mode: local HashMap uniqueness only (all existing tests pass).
    /// Some(store) → registration + createRoom call crate::substrate::barrier::claim() BEFORE
    /// local insert.  NOT feature-gated: testable with MemClaimStore in the default build.
    /// Set via AppState::with_barrier_store() or injected in cluster startup (main.rs).
    pub barrier_store: Option<Arc<dyn ClaimStore + Send + Sync>>,

    /// Username rename map: orig_localpart → new full user_id "@<new>:<server>".
    /// Populated by apply_username_loss() when the ReconcileDriver elects this node's
    /// account as the loser.  whoami and login consult this map to return the new
    /// identity for old tokens (tok_<orig_localpart>).
    /// Node-local: cross-node propagation of the renamed map is not implemented here.
    pub renamed: Mutex<HashMap<String, String>>,

    /// E2EE key management + durability — see the E2ee struct doc comment.
    pub e2ee: E2ee,

    /// sendToDevice relay queue + gossip dedup — see the ToDevice struct doc comment.
    pub to_device: ToDevice,

    /// HMAC-SHA256 signing key for access tokens.
    /// Injected at construction from MATRIX_HS_TOKEN_SECRET env or via
    /// AppState::with_token_secret() (for tests).  Falls back to the process-global
    /// ephemeral key if neither is set (dev-only, tokens don't survive restart).
    pub token_secret: Vec<u8>,

    /// Optional shared secret gating self-registration (internal-task: close open registration
    /// on publicly reachable nodes). None (default) preserves existing open-registration
    /// behaviour — all pre-existing tests and deployments are unaffected.
    /// When Some(secret): every POST /register call must include a matching
    /// `registration_secret` field in its JSON body, compared in constant time; a
    /// missing/wrong secret → 403 M_FORBIDDEN before any UIA session is issued.
    /// Set via MATRIX_HS_REGISTRATION_SHARED_SECRET env, or injected directly with
    /// AppState::with_registration_shared_secret() (tests — avoids the env-var race
    /// a global process env would create across parallel `cargo test` threads).
    pub registration_shared_secret: Option<String>,

    // appservice:start
    //   purpose: Optional application-service configuration — the agent socket
    //            (ROADMAP Phase 3 item 3, AGENT-USE-CASES Case 2). When set,
    //            a holder of `token` can register users under `prefix` with NO
    //            UIA (POST /register with an AS bearer) and mint per-device
    //            tokens for them with NO password (POST /login type
    //            m.login.application_service). That is the "one MXID per
    //            tenant, workers as devices" shape: a fleet worker is just a
    //            login with its own device_id; a worker dying is it stopping
    //            syncing — nothing to clean up.
    //   input:   env MATRIX_HS_AS_TOKEN + MATRIX_HS_AS_PREFIX (or with_appservice in tests)
    //   output:  none (data)
    //   sideEffects: none
    // appservice:end
    pub appservice: Option<AppServiceConfig>,

    /// VoIP TURN configuration (see TurnConfig). None → GET /voip/turnServer
    /// returns an empty {} (no VoIP), unchanged from the pre-existing stub.
    /// Some → the endpoint mints ephemeral TURN credentials. Set from env at
    /// construction (TurnConfig::from_env), or injected in tests via
    /// AppState::with_turn_config (avoids the process-env race across parallel
    /// test threads, same rationale as registration_shared_secret above).
    pub turn: Option<TurnConfig>,

    /// Deactivated users: set of localparts that have been deactivated.
    /// Deactivated accounts are removed from users but remembered here so
    /// login/registration can return appropriate errors if needed in future.
    /// Currently, once removed from users, login fails with M_FORBIDDEN and
    /// re-registration succeeds (slot freed).
    pub deactivated: Mutex<std::collections::HashSet<String>>,

    /// This node's ed25519 signing key (node_id == server_name).  Every locally-created
    /// PDU is signed with this before being added to a RoomLog (see routes/send.rs) —
    /// P1.1 internal-task: closes the self-asserted-sender / node→node forgery gap.
    pub signer: Arc<NodeSigner>,

    /// TOFU grow-set of node_id → ed25519 pubkey.  Pre-seeded with this node's own
    /// pubkey at construction.  Cluster mode additionally grows this via peer key
    /// announcements (see main.rs) — TOFU key distribution is NOT sound until the mesh
    /// itself is authenticated (see internal-task); the sender-binding check in
    /// Pdu::verify_sig limits the blast radius of a race-claimed node_id in the
    /// meantime (a claimed key can only forge senders on ITS OWN claimed node_id
    /// namespace, not arbitrary domains).
    pub key_store: Arc<NodeKeyStore>,

    /// Zenoh cluster layer — None in default (single-node) mode.
    ///
    /// `Arc` because `ClusterState::start_discovery` spawns a background task that
    /// must keep the state alive independently of this `AppState`.
    #[cfg(feature = "cluster")]
    pub cluster: Option<Arc<ClusterState>>,

    /// Ephemeral EDUs (typing / receipts / read markers) — see the Ephemeral
    /// struct doc comment.
    pub ephemeral: Ephemeral,

    /// Media repository — see the MediaStore struct doc comment.
    pub media: MediaStore,

    /// Registered pushers (push notifications feature) — see the PushState
    /// struct doc comment.
    pub push: PushState,

    /// Account data + room tags — see the AccountData struct doc comment.
    pub account_data: AccountData,

    /// DC++-style Lua scripting engine (hook surface: on_room_visible, ...).
    /// Default is empty (no hooks → fully spec-compliant). Populated at startup
    /// from MATRIX_HS_SCRIPTS_DIR by main.rs; hot-reloadable via load_dir.
    pub scripting: crate::scripting::Scripting,
}

// timeline_max_events_from_env:start
//   purpose: Read the Phase 1 GC retention cap from MATRIX_HS_TIMELINE_MAX_EVENTS.
//            0 (the default when unset/unparseable) means unlimited, preserving the
//            original append-only behaviour. Shared by EVERY constructor, including
//            with_server_name: a constructor that hardcoded 0 here would silently
//            remove the only bound there is between a deployment and unbounded
//            growth, and the bound is invisible in the process once it is gone.
//   input:  none
//   output: usize
//   sideEffects: reads an environment variable
// timeline_max_events_from_env:end
fn timeline_max_events_from_env() -> usize {
    std::env::var("MATRIX_HS_TIMELINE_MAX_EVENTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

// roomlog_max_events_from_env:start
//   purpose: Read the RoomLog GC cap from MATRIX_HS_ROOMLOG_MAX_EVENTS. 0 (default)
//            means unlimited. Kept separate from the timeline cap because they bound
//            different things and have different risks: trimming the timeline only
//            shortens what clients can scroll back to and is reversible from the log,
//            whereas collecting the log discards events permanently and cluster-wide.
//   input:  none
//   output: usize
//   sideEffects: reads an environment variable
// roomlog_max_events_from_env:end
fn roomlog_max_events_from_env() -> usize {
    std::env::var("MATRIX_HS_ROOMLOG_MAX_EVENTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

impl Default for AppState {
    fn default() -> Self {
        let server_name =
            std::env::var("MATRIX_HS_SERVER_NAME").unwrap_or_else(|_| "localhost".to_string());
        let public_base_url = std::env::var("MATRIX_HS_PUBLIC_BASEURL").ok();
        let data_dir = std::env::var("MATRIX_HS_DATA_DIR").ok().map(PathBuf::from);
        let token_secret = crate::auth::TokenSecret::global().bytes().to_vec();
        let registration_shared_secret = std::env::var("MATRIX_HS_REGISTRATION_SHARED_SECRET").ok();
        let (signer, key_store) = build_signer(&server_name, data_dir.as_deref());

        AppState {
            rooms: Mutex::new(HashMap::new()),
            room_state: Mutex::new(HashMap::new()),
            aliases: Mutex::new(HashMap::new()),
            alias_provisional: Mutex::new(HashSet::new()),
            stream_pos: Arc::new(AtomicU64::new(0)),
            hlc: Arc::new(AtomicU64::new(0)),
            notify: Arc::new(tokio::sync::Notify::new()),
            room_timeline: Mutex::new(HashMap::new()),
            redactions: Mutex::new(HashMap::new()),
            server_name,
            public_base_url,
            users: Mutex::new(HashMap::new()),
            uia_sessions: Mutex::new(HashSet::new()),
            uia_seq: AtomicU64::new(0),
            persist: PersistCtx::new(data_dir),
            barrier_store: None,
            renamed: Mutex::new(HashMap::new()),
            e2ee: E2ee::default(),
            to_device: ToDevice::default(),
            token_secret,
            registration_shared_secret,
            turn: TurnConfig::from_env(),
            appservice: AppServiceConfig::from_env(),
            deactivated: Mutex::new(std::collections::HashSet::new()),
            signer,
            key_store,
            #[cfg(feature = "cluster")]
            cluster: None,
            ephemeral: Ephemeral::default(),
            media: MediaStore::default(),
            push: PushState::default(),
            account_data: AccountData::default(),
            scripting: crate::scripting::Scripting::default(),
            timeline_max_events: timeline_max_events_from_env(),
            roomlog_max_events: roomlog_max_events_from_env(),
        }
    }
}

impl AppState {
    // AppState::new:start
    //   purpose: Construct a fresh AppState with an empty room map (single-node mode).
    //            Reads MATRIX_HS_SERVER_NAME and MATRIX_HS_PUBLIC_BASEURL from environment.
    //   input:  none
    //   output: Arc<AppState>
    //   sideEffects: reads environment variables
    // AppState::new:end
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    // AppState::with_server_name:start
    //   purpose: Construct an AppState with an explicit server_name.
    //            Reads the two retention caps from the environment, like every other
    //            constructor; see with_server_name_and_caps to pass them explicitly.
    //   input:  name — server name string (e.g. "localhost")
    //   output: Arc<AppState>
    //   sideEffects: reads the two cap environment variables
    // AppState::with_server_name:end
    pub fn with_server_name(name: String) -> Arc<Self> {
        Self::with_server_name_and_caps(
            name,
            timeline_max_events_from_env(),
            roomlog_max_events_from_env(),
        )
    }

    // AppState::with_server_name_and_caps:start
    //   purpose: Construct an AppState with an explicit server_name AND explicit
    //            retention caps. This is the constructor with_server_name delegates
    //            to, and the one a test should call when it wants a specific cap
    //            without touching the process environment: environment variables
    //            are process-global, so a test that sets one to pin a cap also
    //            changes it for every other test running in parallel.
    //            Passing the caps in is also the reason with_server_name can read
    //            them from the environment without creating a hidden dependency:
    //            the values have exactly one place they enter the state.
    //   input:  name — server name string
    //            timeline_max_events — retention cap for the per-room timeline
    //                                    (0 = unlimited)
    //            roomlog_max_events — GC cap for the RoomLog CRDT set (0 = unlimited)
    //   output: Arc<AppState>
    //   sideEffects: none
    // AppState::with_server_name_and_caps:end
    pub fn with_server_name_and_caps(
        name: String,
        timeline_max_events: usize,
        roomlog_max_events: usize,
    ) -> Arc<Self> {
        let token_secret = crate::auth::TokenSecret::global().bytes().to_vec();
        let (signer, key_store) = build_signer(&name, None);
        Arc::new(AppState {
            rooms: Mutex::new(HashMap::new()),
            room_state: Mutex::new(HashMap::new()),
            aliases: Mutex::new(HashMap::new()),
            alias_provisional: Mutex::new(HashSet::new()),
            stream_pos: Arc::new(AtomicU64::new(0)),
            hlc: Arc::new(AtomicU64::new(0)),
            notify: Arc::new(tokio::sync::Notify::new()),
            room_timeline: Mutex::new(HashMap::new()),
            redactions: Mutex::new(HashMap::new()),
            server_name: name,
            public_base_url: None,
            users: Mutex::new(HashMap::new()),
            uia_sessions: Mutex::new(HashSet::new()),
            uia_seq: AtomicU64::new(0),
            persist: PersistCtx::new(None),
            barrier_store: None,
            renamed: Mutex::new(HashMap::new()),
            e2ee: E2ee::default(),
            to_device: ToDevice::default(),
            token_secret,
            registration_shared_secret: None,
            turn: None,
            appservice: None,
            deactivated: Mutex::new(std::collections::HashSet::new()),
            signer,
            key_store,
            #[cfg(feature = "cluster")]
            cluster: None,
            ephemeral: Ephemeral::default(),
            media: MediaStore::default(),
            push: PushState::default(),
            account_data: AccountData::default(),
            scripting: crate::scripting::Scripting::default(),
            timeline_max_events,
            roomlog_max_events,
        })
    }

    // AppState::with_data_dir:start
    //   purpose: Construct an AppState with persistence enabled at the given directory.
    //            Reads MATRIX_HS_SERVER_NAME from env; used by tests and main.rs.
    //   input:  data_dir — PathBuf pointing at the data directory
    //   output: Arc<AppState>
    //   sideEffects: creates directory structure via PersistCtx::new
    // AppState::with_data_dir:end
    pub fn with_data_dir(data_dir: PathBuf) -> Arc<Self> {
        let server_name =
            std::env::var("MATRIX_HS_SERVER_NAME").unwrap_or_else(|_| "localhost".to_string());
        let public_base_url = std::env::var("MATRIX_HS_PUBLIC_BASEURL").ok();
        let token_secret = crate::auth::TokenSecret::global().bytes().to_vec();
        let registration_shared_secret = std::env::var("MATRIX_HS_REGISTRATION_SHARED_SECRET").ok();
        let (signer, key_store) = build_signer(&server_name, Some(&data_dir));
        Arc::new(AppState {
            rooms: Mutex::new(HashMap::new()),
            room_state: Mutex::new(HashMap::new()),
            aliases: Mutex::new(HashMap::new()),
            alias_provisional: Mutex::new(HashSet::new()),
            stream_pos: Arc::new(AtomicU64::new(0)),
            hlc: Arc::new(AtomicU64::new(0)),
            notify: Arc::new(tokio::sync::Notify::new()),
            room_timeline: Mutex::new(HashMap::new()),
            redactions: Mutex::new(HashMap::new()),
            server_name,
            public_base_url,
            users: Mutex::new(HashMap::new()),
            uia_sessions: Mutex::new(HashSet::new()),
            uia_seq: AtomicU64::new(0),
            persist: PersistCtx::new(Some(data_dir)),
            barrier_store: None,
            renamed: Mutex::new(HashMap::new()),
            e2ee: E2ee::default(),
            to_device: ToDevice::default(),
            token_secret,
            registration_shared_secret,
            turn: TurnConfig::from_env(),
            appservice: AppServiceConfig::from_env(),
            deactivated: Mutex::new(std::collections::HashSet::new()),
            signer,
            key_store,
            #[cfg(feature = "cluster")]
            cluster: None,
            ephemeral: Ephemeral::default(),
            media: MediaStore::default(),
            push: PushState::default(),
            account_data: AccountData::default(),
            scripting: crate::scripting::Scripting::default(),
            timeline_max_events: timeline_max_events_from_env(),
            roomlog_max_events: roomlog_max_events_from_env(),
        })
    }

    // AppState::with_cluster:start
    //   purpose: Construct an AppState in cluster mode.
    //            The ClusterConfig carries an open zenoh::Session, a key prefix, and this
    //            node's server_name.  Handlers that run under this state publish/drain
    //            CRDT deltas via Zenoh.
    //            server_name comes from cfg.server_name (NOT the process-global
    //            MATRIX_HS_SERVER_NAME env var) so two cluster nodes constructed in the
    //            SAME process (cluster_test.rs) get distinct node identities; it is used
    //            both as AppState.server_name and as the NodeSigner node_id.
    //   input:  cfg — ClusterConfig (cluster feature only)
    //   output: Arc<AppState>
    //   sideEffects: none (session already open by caller); may create/read
    //                <data_dir>/node_ed25519.key if MATRIX_HS_DATA_DIR is set
    // AppState::with_cluster:end
    #[cfg(feature = "cluster")]
    pub fn with_cluster(cfg: ClusterConfig) -> Arc<Self> {
        let server_name = cfg.server_name.clone();
        let public_base_url = std::env::var("MATRIX_HS_PUBLIC_BASEURL").ok();
        let data_dir = std::env::var("MATRIX_HS_DATA_DIR").ok().map(PathBuf::from);
        let token_secret = crate::auth::TokenSecret::global().bytes().to_vec();
        let registration_shared_secret = std::env::var("MATRIX_HS_REGISTRATION_SHARED_SECRET").ok();
        let (signer, key_store) = build_signer(&server_name, data_dir.as_deref());

        Arc::new(AppState {
            rooms: Mutex::new(HashMap::new()),
            room_state: Mutex::new(HashMap::new()),
            aliases: Mutex::new(HashMap::new()),
            alias_provisional: Mutex::new(HashSet::new()),
            stream_pos: Arc::new(AtomicU64::new(0)),
            hlc: Arc::new(AtomicU64::new(0)),
            notify: Arc::new(tokio::sync::Notify::new()),
            room_timeline: Mutex::new(HashMap::new()),
            redactions: Mutex::new(HashMap::new()),
            server_name,
            public_base_url,
            users: Mutex::new(HashMap::new()),
            uia_sessions: Mutex::new(HashSet::new()),
            uia_seq: AtomicU64::new(0),
            persist: PersistCtx::new(data_dir),
            barrier_store: None,
            renamed: Mutex::new(HashMap::new()),
            e2ee: E2ee::default(),
            to_device: ToDevice::default(),
            token_secret,
            registration_shared_secret,
            turn: TurnConfig::from_env(),
            appservice: AppServiceConfig::from_env(),
            deactivated: Mutex::new(std::collections::HashSet::new()),
            signer,
            key_store,
            cluster: {
                // Arc first, then start_discovery — the wildcard subscriber task holds
                // a clone, so the cluster layer outlives any single request handler.
                let cluster = Arc::new(ClusterState::new(cfg));
                cluster.start_discovery();
                Some(cluster)
            },
            ephemeral: Ephemeral::default(),
            media: MediaStore::default(),
            push: PushState::default(),
            account_data: AccountData::default(),
            scripting: crate::scripting::Scripting::default(),
            timeline_max_events: timeline_max_events_from_env(),
            roomlog_max_events: roomlog_max_events_from_env(),
        })
    }

    // AppState::with_barrier_store:start
    //   purpose: Builder-style setter that injects a ClaimStore into an AppState
    //            constructed by new()/with_server_name()/with_data_dir() — before wrapping
    //            in Arc.  Used by tests (inject MemClaimStore) and by build_state cluster
    //            branch (inject KvFencedClaimStore).
    //            Consumes the Arc<AppState>, temporarily takes ownership, sets the field,
    //            and returns the Arc.  This is safe because the Arc was just created and
    //            has a single strong reference at call time.
    //   input:  state — Arc<AppState> (strong refcount == 1 at call time);
    //           store — Arc<dyn ClaimStore + Send + Sync>
    //   output: Arc<AppState> with barrier_store set
    //   sideEffects: mutates barrier_store field via Arc::get_mut (panics if refcount > 1)
    // AppState::with_barrier_store:end
    pub fn with_barrier_store(
        mut state: Arc<Self>,
        store: Arc<dyn ClaimStore + Send + Sync>,
    ) -> Arc<Self> {
        Arc::get_mut(&mut state)
            .expect("with_barrier_store: Arc refcount > 1 — call before sharing the Arc")
            .barrier_store = Some(store);
        state
    }

    // AppState::with_token_secret:start
    //   purpose: Builder-style setter that overrides the token_secret on a freshly
    //            constructed Arc<AppState> (refcount == 1).  Used in tests to inject
    //            a deterministic secret without touching the process-global OnceLock.
    //   input:  state — Arc<AppState> (strong refcount == 1);
    //           secret — Vec<u8> HMAC key
    //   output: Arc<AppState> with token_secret replaced
    //   sideEffects: mutates token_secret via Arc::get_mut (panics if refcount > 1)
    // AppState::with_token_secret:end
    pub fn with_token_secret(mut state: Arc<Self>, secret: Vec<u8>) -> Arc<Self> {
        Arc::get_mut(&mut state)
            .expect("with_token_secret: Arc refcount > 1")
            .token_secret = secret;
        state
    }

    // AppState::with_registration_shared_secret:start
    //   purpose: Builder-style setter that injects a registration_shared_secret on a
    //            freshly constructed Arc<AppState> (refcount == 1).  Used by tests to
    //            exercise the internal-task registration gate without setting a process-global
    //            env var (which would race across parallel `cargo test` threads).
    //   input:  state — Arc<AppState> (strong refcount == 1);
    //           secret — the shared secret string clients must echo back
    //   output: Arc<AppState> with registration_shared_secret set
    //   sideEffects: mutates registration_shared_secret via Arc::get_mut (panics if
    //                refcount > 1)
    // AppState::with_registration_shared_secret:end
    pub fn with_registration_shared_secret(mut state: Arc<Self>, secret: String) -> Arc<Self> {
        Arc::get_mut(&mut state)
            .expect("with_registration_shared_secret: Arc refcount > 1")
            .registration_shared_secret = Some(secret);
        state
    }

    // AppState::with_appservice:start
    //   purpose: Builder-style setter injecting an AppServiceConfig on a freshly
    //            constructed Arc<AppState> (refcount == 1). Tests use this
    //            instead of MATRIX_HS_AS_* env vars, which would race across
    //            parallel cargo-test threads (same rationale as
    //            with_registration_shared_secret).
    //   input:  state — Arc<AppState> (strong refcount == 1); cfg — AppServiceConfig
    //   output: Arc<AppState> with appservice set
    //   sideEffects: mutates via Arc::get_mut (panics if refcount > 1)
    // AppState::with_appservice:end
    pub fn with_appservice(mut state: Arc<Self>, cfg: AppServiceConfig) -> Arc<Self> {
        Arc::get_mut(&mut state)
            .expect("with_appservice: Arc refcount > 1")
            .appservice = Some(cfg);
        state
    }

    // AppState::with_turn_config:start
    //   purpose: Builder-style setter that injects a TurnConfig on a freshly
    //            constructed Arc<AppState> (refcount == 1).  Used by tests to
    //            exercise the VoIP turnServer endpoint without setting process-
    //            global MATRIX_HS_TURN_* env vars (which would race across
    //            parallel `cargo test` threads — same rationale as
    //            with_registration_shared_secret).
    //   input:  state — Arc<AppState> (strong refcount == 1); cfg — TurnConfig
    //   output: Arc<AppState> with turn set to Some(cfg)
    //   sideEffects: mutates turn via Arc::get_mut (panics if refcount > 1)
    // AppState::with_turn_config:end
    pub fn with_turn_config(mut state: Arc<Self>, cfg: TurnConfig) -> Arc<Self> {
        Arc::get_mut(&mut state)
            .expect("with_turn_config: Arc refcount > 1")
            .turn = Some(cfg);
        state
    }

    // AppState::register_user:start
    //   purpose: Insert a new user into the local user store.
    //            Returns Err(()) if the username is already taken — the caller
    //            converts this to a 400 M_USER_IN_USE response.
    //            The `provisional` flag is set true when the barrier returned
    //            Provisional{fence} (coordinator unreachable, AP path) — the account is
    //            locally valid but subject to reconcile() on partition heal.
    //            password_hash MUST be an Argon2id PHC string produced by
    //            auth::hash_password(); plaintext passwords must never be stored here.
    //   input:  username — localpart string;
    //           password_hash — Argon2id PHC hash string (from auth::hash_password());
    //           device_id — device identifier string;
    //           provisional — true if inserted under AP-optimistic barrier outcome
    //   output: Ok(()) on success; Err(()) if username already exists or mutex poisoned
    //   sideEffects: inserts into self.users
    // AppState::register_user:end
    #[allow(clippy::result_unit_err)]
    pub fn register_user(
        &self,
        username: &str,
        password_hash: &str,
        device_id: &str,
        provisional: bool,
    ) -> Result<(), ()> {
        let mut users = self.users.lock().map_err(|_| ())?;
        if users.contains_key(username) {
            return Err(());
        }
        users.insert(
            username.to_string(),
            UserRecord {
                password_hash: password_hash.to_string(),
                device_id: device_id.to_string(),
                provisional,
                rename_required: false,
                epoch: 0,
            },
        );
        Ok(())
    }

    // AppState::deterministic_new_localpart:start
    //   purpose: Compute the deterministic new localpart for a losing username claim.
    //            Scheme: "<orig_localpart>--<loser_server>" where loser_server is the
    //            homeserver part of the losing claimant MXID ("@user:server" → "server").
    //            This is a pure function of the losing claim — no randomness, no clock —
    //            so every node computes the same result identically (coordination-free).
    //            Assumption: loser_claimant is a well-formed MXID "@<user>:<server>".
    //            If the server part cannot be extracted (malformed MXID), falls back to
    //            "<orig>--lost" to guarantee a rename always occurs.
    //   input:  orig_localpart — the original username localpart (e.g. "alice");
    //           loser_claimant — the losing MXID (e.g. "@alice:node-42")
    //   output: String — new localpart (e.g. "alice--node-42")
    //   sideEffects: none
    // AppState::deterministic_new_localpart:end
    pub fn deterministic_new_localpart(orig_localpart: &str, loser_claimant: &str) -> String {
        // Parse "@user:server" → "server".  Strip leading '@', split on ':', take tail.
        let server_part = loser_claimant
            .strip_prefix('@')
            .and_then(|s| s.split_once(':').map(|x| x.1))
            .unwrap_or("lost");
        format!("{orig_localpart}--{server_part}")
    }

    // AppState::apply_username_loss:start
    //   purpose: Apply a coordination-free rename for the losing account in a grow-set
    //            username conflict.  Called by the ReconcileDriver loser handler.
    //            Actions (all under a single users-lock):
    //              1. Look up orig_localpart in users (no-op if absent — not our account).
    //              2. Compute new_localpart via deterministic_new_localpart.
    //              3. Move the UserRecord from orig_localpart to new_localpart in users;
    //                 clear rename_required on the new record.
    //              4. Record orig_localpart → "@<new_localpart>:<server>" in renamed map.
    //            After this call:
    //              - users["alice"] is gone; users["alice--node-42"] is present.
    //              - renamed["alice"] = "@alice--node-42:localhost".
    //              - register_user("alice") succeeds (slot is free again).
    //              - whoami(tok_alice) returns "@alice--node-42:localhost" (via renamed map).
    //            Only renames when THIS node actually holds the orig account in users.
    //            Winners are untouched (their localpart is not in users as orig on this node
    //            when the loser fires, OR when orig == winner we simply skip).
    //   input:  orig_localpart — the localpart that lost (e.g. "alice");
    //           loser_claimant — the losing MXID (e.g. "@alice:node-42"), used for scheme;
    //           winner_claimant — the winning MXID (for the log entry)
    //   output: Ok(()) on success; Err(String) on mutex poison
    //   sideEffects: mutates users (move) + renamed (insert); writes to stderr
    // AppState::apply_username_loss:end
    pub fn apply_username_loss(
        &self,
        orig_localpart: &str,
        loser_claimant: &str,
        winner_claimant: &str,
    ) -> Result<(), String> {
        let new_localpart = Self::deterministic_new_localpart(orig_localpart, loser_claimant);
        let new_user_id = format!("@{}:{}", new_localpart, self.server_name);

        // ── Move the UserRecord under the users lock ───────────────────────────
        let mut users = self.users.lock().map_err(|e| e.to_string())?;

        // Only act if this node actually holds the losing account.
        let record = match users.remove(orig_localpart) {
            Some(r) => r,
            None => {
                // Account not found locally — not our problem (winner-side node, or already
                // renamed).  Log and return cleanly.
                eprintln!(
                    "[matrix-hs] apply_username_loss: {orig_localpart:?} not found locally \
                     (winner={winner_claimant:?}) — skipping rename"
                );
                return Ok(());
            }
        };

        // Insert under the new localpart with rename_required cleared.
        let new_record = UserRecord {
            rename_required: false,
            ..record
        };
        users.insert(new_localpart.clone(), new_record);
        drop(users);

        // ── Record the old→new mapping in renamed ─────────────────────────────
        let mut renamed = self.renamed.lock().map_err(|e| e.to_string())?;
        renamed.insert(orig_localpart.to_string(), new_user_id.clone());
        drop(renamed);

        // Wake any long-polling /sync so the org.mrgd.renamed push field (routes/sync.rs)
        // is seen on this poll rather than only after the client's next timeout elapses.
        self.notify.notify_waiters();

        eprintln!(
            "[matrix-hs] RENAMED: {orig_localpart:?} → {new_localpart:?} ({new_user_id:?}) \
             lost grow-set conflict to winner={winner_claimant:?}. \
             Client discovers rename via GET /whoami on next poll. \
             Already-sent events keep old sender field (re-attribution out of scope). \
             renamed map is node-local (cross-node propagation not implemented)."
        );
        Ok(())
    }

    // AppState::mark_rename_required:start
    //   purpose: Compatibility shim that delegates to apply_username_loss.
    //            Kept so the loser-handler call site in main.rs compiles without change
    //            during a refactor.  New callers should use apply_username_loss directly.
    //            loser_claimant defaults to "@<username>:<server_name>" when not provided
    //            here; see the main.rs loser handler for the full call with loser_claimant.
    //   input:  username — orig localpart; winner_claimant — winning MXID
    //   output: Ok(()) on success; Err(String) on error
    //   sideEffects: delegates to apply_username_loss
    // AppState::mark_rename_required:end
    pub fn mark_rename_required(
        &self,
        username: &str,
        winner_claimant: &str,
    ) -> Result<(), String> {
        // Reconstruct loser_claimant as "@<username>:<server_name>" — this is what
        // register.rs passes to crate::substrate::barrier::claim as the claimant string.
        let loser_claimant = format!("@{}:{}", username, self.server_name);
        self.apply_username_loss(username, &loser_claimant, winner_claimant)
    }

    // AppState::mark_alias_relinquished:start
    //   purpose: Relinquish a room alias that lost a grow-set uniqueness conflict.
    //            Called by the ReconcileDriver loser handler when this node holds the
    //            losing claimant for an mx:alias: key.  Removes the alias from the
    //            local aliases map (so this node no longer serves it) and removes it
    //            from alias_provisional.  The room itself is unaffected.
    //            Full re-point flow (new alias assignment, room directory update, client
    //            notification) is OUT OF SCOPE for this milestone — flag + eprintln only.
    //            Same posture as mark_rename_required for usernames (SPEC §8.6).
    //   input:  alias — full alias string (e.g. "#foo:localhost") to relinquish;
    //           winner_claimant — the room_id that won the conflict (for the log entry)
    //   output: Ok(()) on success; Err(String) if mutex poisoned
    //   sideEffects: removes alias from self.aliases; removes from self.alias_provisional;
    //                writes to stderr
    // AppState::mark_alias_relinquished:end
    pub fn mark_alias_relinquished(
        &self,
        alias: &str,
        winner_claimant: &str,
    ) -> Result<(), String> {
        let mut aliases = self.aliases.lock().map_err(|e| e.to_string())?;
        let relinquished_room_id = aliases.remove(alias);
        drop(aliases);

        // Remove from provisional set if it was there.
        if let Ok(mut prov) = self.alias_provisional.lock() {
            prov.remove(alias);
        }

        // A room continuing to advertise a canonical_alias the directory no longer
        // resolves back to it is a worse inconsistency than the room simply having
        // none — clear it if it still names the alias just lost. Guarded on the
        // content actually matching: an unrelated, still-valid canonical alias (this
        // room was re-pointed since, or never was this one) must be left untouched.
        // Node-local, unreplicated — same posture as the aliases-map removal above:
        // no PDU is signed for this, so a peer that also held this alias resolves
        // its own copy of the same conflict independently. Visible on a client's
        // next FULL /sync (state is always sent whole there); not synthesized into
        // an in-flight incremental /sync, which would need a synthetic event_id for
        // a state change with no underlying signed event.
        if let Some(room_id) = relinquished_room_id {
            if let Ok(mut rs) = self.room_state.lock() {
                if let Some(events) = rs.get_mut(&room_id) {
                    if let Some(canonical) = events.iter_mut().find(|ev| {
                        ev.event_type == "m.room.canonical_alias" && ev.state_key.is_empty()
                    }) {
                        let names_lost_alias =
                            canonical.content.get("alias").and_then(|v| v.as_str()) == Some(alias);
                        if names_lost_alias {
                            canonical.content = serde_json::json!({});
                        }
                    }
                }
            }
        }

        eprintln!(
            "[matrix-hs] ALIAS_RELINQUISHED: alias={alias:?} \
             lost grow-set conflict to winner={winner_claimant:?}. \
             Cleared this room's stale canonical_alias, if it named this alias. \
             Full re-point (new alias assignment, client notification) deferred — \
             see [see docs/DESIGN.md]"
        );
        Ok(())
    }

    // AppState::issue_uia_session:start
    //   purpose: Mint a new UIA session ID and store it in uia_sessions.
    //            The ID format is "uia_<N>" where N is a monotonically increasing u64.
    //            No Math/rand dependency — the AtomicU64 counter is sufficient for
    //            in-process uniqueness.
    //   input:  none
    //   output: Ok(String) — the new session ID; Err(String) on mutex poison
    //   sideEffects: increments uia_seq; inserts into self.uia_sessions
    // AppState::issue_uia_session:end
    pub fn issue_uia_session(&self) -> Result<String, String> {
        let n = self.uia_seq.fetch_add(1, Ordering::Relaxed);
        let id = format!("uia_{n}");
        let mut sessions = self.uia_sessions.lock().map_err(|e| e.to_string())?;
        sessions.insert(id.clone());
        Ok(id)
    }

    // AppState::consume_uia_session:start
    //   purpose: Check if a UIA session ID is valid and remove it (one-shot use).
    //            Returns true if the session was present and has been consumed.
    //            Returns false if the session ID is unknown.
    //   input:  session_id — string to look up
    //   output: Ok(bool) — true = consumed; false = not found; Err(String) on mutex poison
    //   sideEffects: removes from self.uia_sessions if present
    // AppState::consume_uia_session:end
    pub fn consume_uia_session(&self, session_id: &str) -> Result<bool, String> {
        let mut sessions = self.uia_sessions.lock().map_err(|e| e.to_string())?;
        Ok(sessions.remove(session_id))
    }

    // AppState::enqueue_to_device:start
    //   purpose: Enqueue one to-device message for (target_user, target_device), assigning
    //            it a fresh position from the SAME global stream_pos counter used for room
    //            sync (so a single since-token space covers both). Idempotent per msg_id:
    //            if msg_id was already seen (to_device_seen), this is a silent no-op — this
    //            is what makes it safe for the PUT handler to enqueue locally AND publish
    //            cross-node gossip unconditionally: the gossip may loop back to the
    //            publishing node's own subscriber (cluster feature) and would otherwise
    //            double-store the same message.
    //   input:  target_user, target_device — recipient identity;
    //           sender — verified sender user_id;
    //           event_type — to-device event type (e.g. "m.room_key_request");
    //           content — event content JSON;
    //           msg_id — caller-computed dedup key, unique per (sender, txn_id, target_user,
    //                    target_device)
    //   output: Ok(()) on success; Err(String) on mutex poison
    //   sideEffects: mutates to_device_seen + to_device_queue; wakes long-poll /sync waiters
    //                via self.notify (shared with room-event delivery)
    // AppState::enqueue_to_device:end
    pub fn enqueue_to_device(
        &self,
        target_user: &str,
        target_device: &str,
        sender: &str,
        event_type: &str,
        content: &Value,
        msg_id: &str,
    ) -> Result<(), String> {
        {
            let mut seen = self
                .to_device
                .to_device_seen
                .lock()
                .map_err(|e| e.to_string())?;
            if !seen.insert(msg_id.to_string()) {
                // Already enqueued (direct send or an earlier gossip delivery) — no-op.
                return Ok(());
            }
        }

        let pos = self.stream_pos.fetch_add(1, Ordering::SeqCst);
        let ev = serde_json::json!({
            "sender":  sender,
            "type":    event_type,
            "content": content,
        });

        let mut queue = self
            .to_device
            .to_device_queue
            .lock()
            .map_err(|e| e.to_string())?;
        queue
            .entry((target_user.to_string(), target_device.to_string()))
            .or_default()
            .push((pos, ev));
        drop(queue);

        self.notify.notify_waiters();
        Ok(())
    }

    // AppState::drain_to_device:start
    //   purpose: Return the pending to-device events for (user_id, device_id) that this
    //            device has not yet received, and GC the entries it has.
    //
    //            The delivery ack is a PER-DEVICE watermark (self.to_device.delivered),
    //            NOT the caller's room-sync since token. This is the load-bearing fix:
    //            to-device messages and room events draw from the SAME global
    //            AppState::stream_pos counter, so on a busy node a to-device message can
    //            be enqueued at a position BELOW the device's room-sync cursor. Using the
    //            since token as the ack (the pre-fix behaviour) deleted such a message —
    //            not yet delivered — on first read: a room_key sent right after an
    //            invite/{{createRoom}} sequence was silently GC'd and never reached the
    //            target device. A device's room since says nothing about which to-device
    //            messages it has actually seen; the only true ack is "drain returned it".
    //
    //            since_pos is still accepted (sync.rs/sliding_sync.rs pass it) but is not
    //            consulted for to-device GC; it is retained only to keep the call site
    //            signature stable and to allow the initial-sync/None path to return the
    //            full pending set.
    //
    //   input:  user_id, device_id — target device identity;
    //           since_pos — ignored for to-device acking (kept for signature stability);
    //                       None for initial sync
    //   output: Ok(Vec<Value>) — event JSON values {"sender","type","content"}, oldest
    //           first, for every queued message the device has not yet had returned;
    //           Err(String) on mutex poison
    //   sideEffects: mutates to_device_queue (removes delivered entries in place) and
    //                to_device.delivered (advances the per-device watermark to the highest
    //                position handed back)
    // AppState::drain_to_device:end
    pub fn drain_to_device(
        &self,
        user_id: &str,
        device_id: &str,
        _since_pos: Option<u64>,
    ) -> Result<Vec<Value>, String> {
        let mut queue = self
            .to_device
            .to_device_queue
            .lock()
            .map_err(|e| e.to_string())?;
        let key = (user_id.to_string(), device_id.to_string());

        let Some(entries) = queue.get_mut(&key) else {
            return Ok(Vec::new());
        };

        let mut delivered = self
            .to_device
            .delivered
            .lock()
            .map_err(|e| e.to_string())?;
        let watermark: Option<u64> = delivered.get(&key).copied();

        // Oldest-first FIFO.
        entries.sort_by_key(|(pos, _)| *pos);

        // What do we hand back? On a device's VERY FIRST drain there is no watermark
        // entry yet — it has never been acked anything, so the whole queue is new.
        // Afterwards only messages strictly above the per-device watermark are new.
        let deliver_entries: Vec<(u64, Value)> = match watermark {
            None => entries.clone(),
            Some(w) => entries
                .iter()
                .filter(|(pos, _)| *pos > w)
                .cloned()
                .collect(),
        };
        let deliver: Vec<Value> = deliver_entries
            .iter()
            .map(|(_, ev)| ev.clone())
            .collect();

        // New watermark = highest position this call returns; if none returned, keep
        // the existing watermark (or 0 for a device that has nothing at all).
        let new_watermark = deliver_entries
            .iter()
            .map(|(pos, _)| *pos)
            .max()
            .unwrap_or(watermark.unwrap_or(0));
        delivered.insert(key, new_watermark);

        // GC everything at or below the new watermark — a message becomes disposable
        // only once drain has actually returned it to this device.
        entries.retain(|(pos, _)| *pos > new_watermark);

        Ok(deliver)
    }

    // AppState::new_media_id:start
    //   purpose: Mint a fresh, unguessable media_id for a POST upload. 24 random bytes
    //            (OsRng) base64url-nopad encoded — same building blocks auth.rs already
    //            uses for token nonces, no new crate. Collision probability is
    //            astronomically small (192 bits of entropy); no uniqueness check against
    //            the media map is performed (mirrors event_id / token generation elsewhere
    //            in this codebase, which also do not re-check).
    //   input:  none
    //   output: String — URL-safe media_id with no path-unsafe characters
    //   sideEffects: reads from OsRng
    // AppState::new_media_id:end
    pub fn new_media_id() -> String {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
        use rand::{rngs::OsRng, RngCore};
        let mut buf = [0u8; 24];
        OsRng.fill_bytes(&mut buf);
        URL_SAFE_NO_PAD.encode(buf)
    }

    // AppState::max_media_upload_bytes:start
    //   purpose: Return the configured maximum upload size in bytes for
    //            GET .../media/config's "m.upload.size" and for upload-size enforcement.
    //            Reads MATRIX_HS_MEDIA_MAX_BYTES once per call (cheap env lookup; no
    //            caching needed — this is not a hot path). Defaults to 50 MiB, a common
    //            Synapse default or of that order of magnitude, when unset or unparsable.
    //   input:  none
    //   output: u64 — max upload size in bytes
    //   sideEffects: reads an environment variable
    // AppState::max_media_upload_bytes:end
    pub fn max_media_upload_bytes(&self) -> u64 {
        std::env::var("MATRIX_HS_MEDIA_MAX_BYTES")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(50 * 1024 * 1024)
    }

    // AppState::store_media:start
    //   purpose: Store one uploaded media blob under `media_id`, owned by this node
    //            (owner_node = self.server_name). Inserts into the in-memory media map
    //            (always) and, when persistence is enabled, fsyncs the blob + a
    //            content-type sidecar to disk via persist_media_blob so it survives a
    //            restart (see persist::replay_media).
    //   input:  media_id — the id to store under (from new_media_id());
    //           content_type — the client-supplied Content-Type (already defaulted by
    //           the caller if absent); bytes — raw file contents
    //   output: Ok(()) on success; Err(String) on mutex poison
    //   sideEffects: inserts into self.media.media; (persist enabled) writes two files under
    //                <data_dir>/media/
    // AppState::store_media:end
    pub fn store_media(
        &self,
        media_id: &str,
        content_type: &str,
        bytes: Vec<u8>,
    ) -> Result<(), String> {
        self.persist_media_blob(media_id, content_type, &bytes);
        // MEMORY (gamma-33, 2026-08-25): when persistence is on, the disk
        // copy is the source of truth and the in-RAM map holds ONLY the index
        // (empty-bytes sentinel) — get_media loads the blob from disk on
        // demand. Retaining every uploaded blob here leaked at the exact rate
        // media arrived: the camera pipeline uploads ~5 MiB clips around the
        // clock, RSS+swap grew 8-9 GiB/day on an otherwise idle node, and the
        // ~20 GiB anon footprint matched the on-disk media dir 1:1. Without
        // persistence (in-memory mode, every non-persist test) the full bytes
        // stay in RAM as before — there is no disk copy to fall back on.
        let ram_bytes = if self.persist.enabled() {
            Arc::new(Vec::new())
        } else {
            Arc::new(bytes)
        };
        let mut guard = self.media.media.lock().map_err(|e| e.to_string())?;
        guard.insert(
            media_id.to_string(),
            MediaEntry {
                content_type: content_type.to_string(),
                bytes: ram_bytes,
                owner_node: self.server_name.clone(),
            },
        );
        Ok(())
    }

    // AppState::get_media:start
    //   purpose: Look up a stored media blob by media_id. Local-node only — does NOT
    //            attempt any cross-node fetch (see routes/media.rs module header for the
    //            deferred cross-node seam); callers that need cross-node behaviour must
    //            invoke that seam themselves on a None result.
    //   input:  media_id
    //   output: Some(MediaEntry) clone if present locally; None if absent or mutex poisoned
    //   sideEffects: none
    // AppState::get_media:end
    pub fn get_media(&self, media_id: &str) -> Option<MediaEntry> {
        let mut entry = self.media.media.lock().ok()?.get(media_id).cloned()?;
        // Persisted mode: the map holds only the index — load the blob from
        // disk on demand (gamma-33: retaining it in RAM leaked at the rate
        // media arrived). An empty index entry whose file is gone (operator
        // pruned data/media) returns None — a missing blob, not an empty one.
        if entry.bytes.is_empty() && self.persist.enabled() {
            let bytes = self.load_media_blob_from_disk(media_id)?;
            entry.bytes = Arc::new(bytes);
        }
        Some(entry)
    }

    // AppState::load_media_blob_from_disk:start
    //   purpose: Read one persisted media blob back from
    //            <data_dir>/media/<sanitized_media_id>. Mirrors
    //            persist_media_blob's layout exactly (same sanitizer, same
    //            filename derivation). Best-effort: any fs error → None, which
    //            get_media surfaces as "not held".
    //   input:  media_id
    //   output: Some(bytes) if the file exists and reads; None otherwise
    //   sideEffects: one file read
    // AppState::load_media_blob_from_disk:end
    fn load_media_blob_from_disk(&self, media_id: &str) -> Option<Vec<u8>> {
        let dir = self.persist.data_dir.as_ref()?;
        let path = dir
            .join("media")
            .join(crate::persist::sanitize_filename(media_id));
        std::fs::read(&path).ok()
    }

    // AppState::cache_media_from_peer:start
    //   purpose: Store a blob fetched from another node. Differs from store_media in
    //            the one way that matters: owner_node is preserved from the peer
    //            rather than overwritten with this node's name, so a cached copy does
    //            not claim to be the original.
    //            Persisted like any upload when a data_dir is set — a blob worth
    //            fetching once is worth surviving a restart, and it means a node that
    //            has served a room's media can keep serving it even if the uploader
    //            never comes back. The trade is disk: every node that reads a blob
    //            eventually keeps it.
    //   input:  media_id; entry — as returned by ClusterState::fetch_media
    //   output: Ok(()) on success; Err(String) on mutex poison
    //   sideEffects: inserts into self.media.media; (persist enabled) writes two files
    //                under <data_dir>/media/
    // AppState::cache_media_from_peer:end
    pub fn cache_media_from_peer(&self, media_id: &str, entry: &MediaEntry) -> Result<(), String> {
        self.persist_media_blob(media_id, &entry.content_type, &entry.bytes);
        // Same index-only rule as store_media under persistence (gamma-33):
        // the bytes were needed to write the disk copy, not to be retained.
        let mut indexed = entry.clone();
        if self.persist.enabled() {
            indexed.bytes = Arc::new(Vec::new());
        }
        let mut guard = self.media.media.lock().map_err(|e| e.to_string())?;
        guard.insert(media_id.to_string(), indexed);
        Ok(())
    }

    // AppState::ensure_room:start
    //   purpose: Ensure the room exists in the rooms map (creates an empty RoomLog if absent).
    //            Called by send-event and createRoom to initialise rooms lazily.
    //   input:  room_id — string key
    //   output: () — room is guaranteed to exist in rooms map after this call
    //   sideEffects: may insert a new empty RoomLog into self.rooms
    // AppState::ensure_room:end
    pub fn ensure_room(&self, room_id: &str) {
        let mut rooms = self.rooms.lock().expect("rooms mutex poisoned");
        rooms.entry(room_id.to_string()).or_default();
    }

    // AppState::ensure_room_state:start
    //   purpose: Ensure both rooms and room_state have entries for room_id.
    //            Called by createRoom and join handlers.
    //   input:  room_id — string key
    //   output: () — both maps have an entry for room_id after this call
    //   sideEffects: may insert into self.rooms and self.room_state
    // AppState::ensure_room_state:end
    pub fn ensure_room_state(&self, room_id: &str) {
        self.ensure_room(room_id);
        let mut rs = self.room_state.lock().expect("room_state mutex poisoned");
        rs.entry(room_id.to_string()).or_default();
        let mut rt = self
            .room_timeline
            .lock()
            .expect("room_timeline mutex poisoned");
        rt.entry(room_id.to_string()).or_default();
    }

    // AppState::resolve_room_id:start
    //   purpose: Resolve a room identifier that may be a room_id ("!id:server") OR a
    //            room alias ("#alias:server") to the canonical room_id.
    //            Room-scoped CS-API endpoints nominally take a room_id, but some real
    //            clients (matrix-nio / matrix-commander --tail) pass the alias directly
    //            in the {roomId} path slot.  Accepting both is a harmless leniency that
    //            broadens client compat; a non-alias input is returned unchanged.
    //   input:  id_or_alias — room_id or alias from the request path
    //   output: String — resolved room_id if id_or_alias is a known alias, else the
    //                     input unchanged.
    //   sideEffects: none (read-only lock of aliases)
    // AppState::resolve_room_id:end
    pub fn resolve_room_id(&self, id_or_alias: &str) -> String {
        if id_or_alias.starts_with('#') {
            if let Ok(aliases) = self.aliases.lock() {
                if let Some(room_id) = aliases.get(id_or_alias) {
                    return room_id.clone();
                }
            }
        }
        id_or_alias.to_string()
    }

    // AppState::base_url_for_request:start
    //   purpose: Return the base URL to advertise to clients.
    //            Uses public_base_url if set, else constructs from host header, else localhost.
    //   input:  host_header — optional Host header value from the request
    //   output: String base URL (e.g. "http://localhost")
    //   sideEffects: none
    // AppState::base_url_for_request:end
    pub fn base_url_for_request(&self, host_header: Option<&str>) -> String {
        if let Some(ref url) = self.public_base_url {
            return url.clone();
        }
        if let Some(host) = host_header {
            return format!("http://{host}");
        }
        format!("http://{}", self.server_name)
    }

    // ── Ephemeral EDUs: typing / receipts / read markers ──────────────────────
    //
    // DESIGN NOTE (typing):
    //   `typing` holds ONLY the user_ids this node itself has been told are typing
    //   (room_id → user_id → expiry_ms).  It is the local source of truth that
    //   put_typing (routes/ephemeral.rs) mutates directly.
    //   `typing_remote` holds the last full snapshot published by EACH remote node
    //   for a room (room_id → node_id → user_id → expiry_ms).  Bucketing by
    //   node_id (rather than flattening into one shared map) means a node's
    //   "stopped typing" update — which republishes its OWN snapshot without that
    //   user — correctly clears the entry on every other node on the next drain,
    //   without needing an explicit tombstone/remove message.  This is a plain
    //   last-writer-wins register per (room_id, node_id): commutative, idempotent,
    //   and self-healing (a lost/duplicated snapshot converges on the next publish).
    //   typing_user_ids() unions the local view with every remote bucket, filtering
    //   out anything past its expiry (lazy expiry at read time — no background
    //   sweep thread needed).
    //
    // DESIGN NOTE (receipts):
    //   `receipts` is a single shared map keyed by room_id → user_id → (receipt_type,
    //   event_id, ts_ms).  Only the newest ts per (room,user,type) key wins — a
    //   plain LWW-register merge, applied identically whether the write is local
    //   (post_receipt/post_read_markers) or came from a remote node via
    //   drain_cluster_ephemeral.  Cross-node conflicts are not expected in practice
    //   (a user's client talks to one home node), but the LWW rule keeps the merge
    //   safe (commutative/idempotent) regardless.
    //
    // DESIGN NOTE (fully_read):
    //   `fully_read` is node-local only — NOT replicated across the cluster.  This
    //   matches the minimal scope requested: read_markers is accepted and recorded
    //   without error, but cross-node fully_read propagation is not implemented.

    // AppState::set_typing:start
    //   purpose: Record (or clear) this node's local view of `user_id` typing in
    //            `room_id`.  typing=true inserts an expiry timestamp clamped to
    //            [1ms, 120s] from now (guards against a client sending 0 or an
    //            absurdly large timeout); typing=false removes the entry outright.
    //   input:  room_id, user_id; typing — start/stop; timeout_ms — client-supplied
    //           timeout (only meaningful when typing=true)
    //   output: Ok(()) on success; Err(String) on mutex poison
    //   sideEffects: mutates self.ephemeral.typing[room_id]
    // AppState::set_typing:end
    pub fn set_typing(
        &self,
        room_id: &str,
        user_id: &str,
        typing: bool,
        timeout_ms: u64,
    ) -> Result<(), String> {
        let mut guard = self.ephemeral.typing.lock().map_err(|e| e.to_string())?;
        let room_map = guard.entry(room_id.to_string()).or_default();
        if typing {
            let capped = timeout_ms.clamp(1, 120_000);
            room_map.insert(user_id.to_string(), now_ms() + capped);
        } else {
            room_map.remove(user_id);
        }
        Ok(())
    }

    // AppState::typing_snapshot_local:start
    //   purpose: Return a clone of this node's local (non-expired) typing map for
    //            `room_id` — the payload cluster mode publishes to remote nodes.
    //   input:  room_id
    //   output: HashMap<user_id, expiry_ms> (empty if room absent or lock poisoned)
    //   sideEffects: none (prunes expired entries from self.ephemeral.typing as a side benefit)
    // AppState::typing_snapshot_local:end
    pub fn typing_snapshot_local(&self, room_id: &str) -> HashMap<String, u64> {
        let now = now_ms();
        match self.ephemeral.typing.lock() {
            Ok(mut guard) => {
                if let Some(room_map) = guard.get_mut(room_id) {
                    room_map.retain(|_, expiry| *expiry > now);
                    room_map.clone()
                } else {
                    HashMap::new()
                }
            }
            Err(_) => HashMap::new(),
        }
    }

    // AppState::merge_typing_remote:start
    //   purpose: Replace the cached typing bucket for `node_id` in `room_id` with a
    //            freshly received snapshot (last-writer-wins per node — see the
    //            design note above).  Called by drain_cluster_ephemeral for every
    //            blob pulled off the "typing" Zenoh key.
    //   input:  room_id; node_id — the publishing node's server_name;
    //           users — that node's full typing snapshot for this room
    //   output: Ok(()) on success; Err(String) on mutex poison
    //   sideEffects: mutates self.ephemeral.typing_remote[room_id][node_id]
    // AppState::merge_typing_remote:end
    pub fn merge_typing_remote(
        &self,
        room_id: &str,
        node_id: &str,
        users: HashMap<String, u64>,
    ) -> Result<(), String> {
        let mut guard = self
            .ephemeral
            .typing_remote
            .lock()
            .map_err(|e| e.to_string())?;
        guard
            .entry(room_id.to_string())
            .or_default()
            .insert(node_id.to_string(), users);
        Ok(())
    }

    // AppState::typing_user_ids:start
    //   purpose: Return the current (non-expired) set of user_ids typing in
    //            `room_id` — the union of this node's local view and every remote
    //            node's last-published bucket.  Expired entries are filtered out
    //            at read time (lazy expiry, no background sweep).  Sorted for a
    //            deterministic response body.
    //   input:  room_id
    //   output: Vec<String> (empty if nobody is typing / on mutex poison)
    //   sideEffects: none
    // AppState::typing_user_ids:end
    pub fn typing_user_ids(&self, room_id: &str) -> Vec<String> {
        let now = now_ms();
        let mut ids: HashSet<String> = HashSet::new();

        if let Ok(guard) = self.ephemeral.typing.lock() {
            if let Some(room_map) = guard.get(room_id) {
                ids.extend(
                    room_map
                        .iter()
                        .filter(|(_, &expiry)| expiry > now)
                        .map(|(u, _)| u.clone()),
                );
            }
        }
        if let Ok(guard) = self.ephemeral.typing_remote.lock() {
            if let Some(by_node) = guard.get(room_id) {
                for room_map in by_node.values() {
                    ids.extend(
                        room_map
                            .iter()
                            .filter(|(_, &expiry)| expiry > now)
                            .map(|(u, _)| u.clone()),
                    );
                }
            }
        }
        let mut out: Vec<String> = ids.into_iter().collect();
        out.sort();
        out
    }

    // AppState::set_receipt:start
    //   purpose: Record a read receipt for (room_id, user_id, receipt_type),
    //            applying last-writer-wins on ts_ms — an older/duplicate/replayed
    //            receipt (ts_ms <= the currently stored one) is a no-op so this is
    //            safe to call from both the local HTTP path and the cluster drain
    //            path without an ordering guarantee between them.
    //   input:  room_id, user_id, receipt_type (e.g. "m.read", "m.read.private"),
    //           event_id — the event the receipt points at, ts_ms — receipt time
    //   output: Ok(true) if this write was newer and got applied; Ok(false) if a
    //           newer/equal receipt was already stored (no-op); Err(String) on
    //           mutex poison
    //   sideEffects: mutates self.ephemeral.receipts[room_id][user_id] when applied
    // AppState::set_receipt:end
    pub fn set_receipt(
        &self,
        room_id: &str,
        user_id: &str,
        receipt_type: &str,
        event_id: &str,
        ts_ms: u64,
    ) -> Result<bool, String> {
        let mut guard = self.ephemeral.receipts.lock().map_err(|e| e.to_string())?;
        let room_map = guard.entry(room_id.to_string()).or_default();
        let key = format!("{user_id}\u{0}{receipt_type}");
        let should_apply = match room_map.get(&key) {
            Some((_, _, existing_ts)) => ts_ms > *existing_ts,
            None => true,
        };
        if should_apply {
            room_map.insert(key, (receipt_type.to_string(), event_id.to_string(), ts_ms));
        }
        Ok(should_apply)
    }

    // AppState::receipt_event_content:start
    //   purpose: Build the content object for an "m.receipt" ephemeral event from
    //            the current receipts recorded for `room_id`:
    //            {event_id: {receipt_type: {user_id: {"ts": ts_ms}}}} — the shape
    //            the Matrix spec requires (grouped by event_id, then receipt type).
    //   input:  room_id
    //   output: Some(Value::Object(...)) if any receipts exist for the room;
    //           None if the room has no receipts yet or the lock is poisoned
    //   sideEffects: none
    // AppState::receipt_event_content:end
    pub fn receipt_event_content(&self, room_id: &str) -> Option<Value> {
        let guard = self.ephemeral.receipts.lock().ok()?;
        let room_map = guard.get(room_id)?;
        if room_map.is_empty() {
            return None;
        }

        // event_id -> receipt_type -> user_id -> {"ts": ts}
        let mut by_event: HashMap<String, HashMap<String, serde_json::Map<String, Value>>> =
            HashMap::new();

        for (key, (receipt_type, event_id, ts)) in room_map.iter() {
            // key is "user_id\0receipt_type"; recover user_id (the part before \0).
            let user_id = key.split('\u{0}').next().unwrap_or(key.as_str());
            let by_type = by_event.entry(event_id.clone()).or_default();
            let by_user = by_type.entry(receipt_type.clone()).or_default();
            by_user.insert(user_id.to_string(), json!({ "ts": ts }));
        }

        let mut content = serde_json::Map::new();
        for (event_id, by_type) in by_event {
            let mut type_obj = serde_json::Map::new();
            for (receipt_type, by_user) in by_type {
                type_obj.insert(receipt_type, Value::Object(by_user));
            }
            content.insert(event_id, Value::Object(type_obj));
        }
        Some(Value::Object(content))
    }

    // AppState::set_fully_read:start
    //   purpose: Record the m.fully_read marker position for (room_id, user_id).
    //            Node-local only — see the design note above the ephemeral EDU
    //            section header (cross-node replication is out of scope here).
    //   input:  room_id, user_id, event_id — the event the marker points at
    //   output: Ok(()) on success; Err(String) on mutex poison
    //   sideEffects: mutates self.ephemeral.fully_read[(room_id, user_id)]
    // AppState::set_fully_read:end
    pub fn set_fully_read(
        &self,
        room_id: &str,
        user_id: &str,
        event_id: &str,
    ) -> Result<(), String> {
        let mut guard = self
            .ephemeral
            .fully_read
            .lock()
            .map_err(|e| e.to_string())?;
        guard.insert(
            (room_id.to_string(), user_id.to_string()),
            event_id.to_string(),
        );
        Ok(())
    }

    // ── E2EE device-list change tracking ──────────────────────────────────────

    // AppState::mark_device_list_changed:start
    //   purpose: Record that `user_id`'s device list changed — called on keys/upload
    //            (device_keys present), register (device add), and deactivate/logout
    //            (device removal). Draws a fresh position from the SAME global
    //            stream_pos counter used for room sync / to-device, so the change is
    //            ordered in the single since-token space; stores it as this user's
    //            latest change-pos (last-writer-wins register — only the most recent
    //            pos matters for "changed since N").
    //   input:  user_id — full MXID whose device list changed
    //   output: Ok(new_pos) on success; Err(String) on mutex poison
    //   sideEffects: mutates self.e2ee.device_list_changes; bumps stream_pos; wakes /sync
    //                long-poll waiters via self.notify
    // AppState::mark_device_list_changed:end
    pub fn mark_device_list_changed(&self, user_id: &str) -> Result<u64, String> {
        let pos = self.stream_pos.fetch_add(1, Ordering::SeqCst);
        {
            let mut guard = self
                .e2ee
                .device_list_changes
                .lock()
                .map_err(|e| e.to_string())?;
            guard.insert(user_id.to_string(), pos);
        }
        self.notify.notify_waiters();
        Ok(pos)
    }

    // AppState::device_list_changes_since:start
    //   purpose: Return every user_id whose device list changed strictly after
    //            `since_pos` (its recorded change-pos > since_pos). Used by /sync and
    //            sliding-sync to compute the candidate set for device_lists.changed —
    //            the caller further filters this down to users sharing a room with
    //            the requester (see users_sharing_room_with).
    //   input:  since_pos — the incoming since-token's stream position
    //   output: Vec<String> of user_ids (unsorted); empty on mutex poison
    //   sideEffects: none
    //
    //   NOTE on the >= (not >) comparison: a since-token's numeric value is
    //   stream_pos.load() at response-build time — i.e. the position the NEXT new
    //   event will receive (fetch_add returns the pre-increment value). So the very
    //   next change after the token is issued gets pos == since_pos, not
    //   pos == since_pos + 1. This mirrors build_join_rooms's identical `pos >= n`
    //   convention for room-timeline events on incremental sync.
    // AppState::device_list_changes_since:end
    pub fn device_list_changes_since(&self, since_pos: u64) -> Vec<String> {
        match self.e2ee.device_list_changes.lock() {
            Ok(guard) => guard
                .iter()
                .filter(|(_, &pos)| pos >= since_pos)
                .map(|(u, _)| u.clone())
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    // AppState::joined_members:start
    //   purpose: Return the user_ids of every CURRENT joined member of `room_id`
    //            (latest m.room.member state event per state_key, membership=="join").
    //            Used by routes/push.rs::dispatch_push to compute the notify
    //            candidate set for a newly-sent message. Mirrors get_room_members'
    //            scan pattern in routes/room_state.rs but returns bare user_ids
    //            instead of full event JSON.
    //   input:  room_id — canonical room_id (already alias-resolved by the caller)
    //   output: Vec<String> of joined user_ids (unsorted, may be empty); empty on
    //           mutex poison or unknown room
    //   sideEffects: none (read-only scan of room_state)
    // AppState::joined_members:end
    pub fn joined_members(&self, room_id: &str) -> Vec<String> {
        let Ok(guard) = self.room_state.lock() else {
            return Vec::new();
        };
        let Some(events) = guard.get(room_id) else {
            return Vec::new();
        };

        // room_state stores one entry per state event ever applied to a state_key;
        // take the LAST (most recent) m.room.member event per state_key to get
        // current membership, same convention as get_room_members's snapshot use
        // elsewhere assumes append-order == recency.
        let mut latest: HashMap<&str, &str> = HashMap::new();
        for ev in events {
            if ev.event_type == "m.room.member" {
                if let Some(membership) = ev.content.get("membership").and_then(|v| v.as_str()) {
                    latest.insert(ev.state_key.as_str(), membership);
                }
            }
        }
        latest
            .into_iter()
            .filter(|(_, membership)| *membership == "join")
            .map(|(user_id, _)| user_id.to_string())
            .collect()
    }

    // AppState::user_membership_in_room:start
    //   purpose: Look up a specific user's current membership state ("join",
    //            "invite", "leave", "ban", ...) in a room, from room_state's
    //            m.room.member event for (room_id, user_id). Building block for
    //            /sync's per-caller room filtering — see routes/sync.rs and
    //            routes/sliding_sync.rs, which previously returned EVERY room in
    //            the server to EVERY caller (including unauthenticated ones)
    //            with no membership check at all — a severe live-confirmed
    //            privacy bug (any registered account, even a brand-new one,
    //            could read every other room's full history via /sync).
    //   input:  room_id, user_id
    //   output: Some(membership_str) if an m.room.member event exists for this
    //           user in this room; None if the room or the membership event is
    //           unknown
    //   sideEffects: none (read-only)
    // AppState::user_membership_in_room:end
    pub fn user_membership_in_room(&self, room_id: &str, user_id: &str) -> Option<String> {
        let guard = self.room_state.lock().ok()?;
        let events = guard.get(room_id)?;
        events
            .iter()
            .find(|ev| ev.event_type == "m.room.member" && ev.state_key == user_id)
            .and_then(|ev| {
                ev.content
                    .get("membership")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            })
    }

    // AppState::mark_redacted:start
    //   purpose: Record that `target_event_id` has been redacted by the given
    //            m.room.redaction client-event JSON. Called once by
    //            routes/redact.rs::put_redact_event right after the redaction event
    //            itself has been inserted into the RoomLog/timeline via
    //            routes/send.rs::insert_pdu. Idempotent: redacting an
    //            already-redacted event just overwrites with the latest redaction
    //            (last write wins — mirrors the CRDT LWW convention used elsewhere
    //            in this module; there is no "un-redact").
    //   input:  target_event_id — the event_id being redacted;
    //           redaction_event — full client-event JSON of the m.room.redaction event
    //   output: Result<(), String> — Err on mutex poison
    //   sideEffects: inserts/overwrites self.redactions[target_event_id]
    // AppState::mark_redacted:end
    // AppState::redaction_target:start
    //   purpose: If `ev` is an m.room.redaction, return the event_id it redacts —
    //            and normalise `ev` so the target is present at the top level.
    //
    //            Two shapes exist. Locally-issued redactions carry `redacts` at the
    //            top level; ones that arrived over the mesh carry it in `content`,
    //            because a Pdu replicates content and nothing else. Accept either,
    //            and leave the event carrying both, so a client sees the same shape
    //            whichever node it happens to be talking to.
    //
    //            Deliberately takes no locks: callers on the replication paths hold
    //            the room_timeline lock while building events, and must not acquire
    //            the redactions lock underneath it (read paths take those two in the
    //            opposite order). Extract here, record with mark_redacted after the
    //            timeline lock is released.
    //   input:  ev — a client-event JSON, mutated in place to carry top-level `redacts`
    //   output: Some(target_event_id) if this is a redaction naming a target, else None
    //   sideEffects: may insert a top-level "redacts" key into `ev`
    // AppState::redaction_target:end
    pub fn redaction_target(ev: &mut Value) -> Option<String> {
        if ev.get("type").and_then(|v| v.as_str()) != Some("m.room.redaction") {
            return None;
        }
        let target = ev
            .get("redacts")
            .and_then(|v| v.as_str())
            .or_else(|| {
                ev.get("content")
                    .and_then(|c| c.get("redacts"))
                    .and_then(|v| v.as_str())
            })?
            .to_string();
        if ev.get("redacts").is_none() {
            if let Some(obj) = ev.as_object_mut() {
                obj.insert("redacts".to_string(), Value::String(target.clone()));
            }
        }
        Some(target)
    }

    pub fn mark_redacted(
        &self,
        target_event_id: &str,
        redaction_event: Value,
    ) -> Result<(), String> {
        let mut guard = self.redactions.lock().map_err(|e| e.to_string())?;
        guard.insert(target_event_id.to_string(), redaction_event);
        Ok(())
    }

    // AppState::apply_redaction:start
    //   purpose: Read-side masking. Given a client-event JSON as stored in
    //            room_timeline, return it unchanged if its event_id has not been
    //            redacted, or a masked copy if it has: content stripped to {} and
    //            unsigned.redacted_because set to the recorded m.room.redaction
    //            event. event_id/type/sender/room_id/origin_server_ts are preserved.
    //            Called by every timeline read path — routes/sync.rs
    //            build_join_rooms, routes/room_state.rs get_room_messages,
    //            routes/sliding_sync.rs build_rooms — so redaction masking is
    //            applied uniformly regardless of which endpoint served the event.
    //   input:  ev — a client-event JSON value (must have an "event_id" field to be
    //           maskable; events without one, e.g. malformed input, pass through
    //           unchanged)
    //   output: Value — ev unchanged, or a masked clone
    //   sideEffects: none (read-only; on mutex poison, fails open and returns ev
    //                unchanged rather than panicking)
    // AppState::apply_redaction:end
    pub fn apply_redaction(&self, ev: &Value) -> Value {
        let Some(event_id) = ev.get("event_id").and_then(|v| v.as_str()) else {
            return ev.clone();
        };
        let Ok(guard) = self.redactions.lock() else {
            return ev.clone();
        };
        let Some(redaction_event) = guard.get(event_id) else {
            return ev.clone();
        };

        json!({
            "event_id":         ev.get("event_id").cloned().unwrap_or(Value::Null),
            "type":             ev.get("type").cloned().unwrap_or(Value::Null),
            "sender":           ev.get("sender").cloned().unwrap_or(Value::Null),
            "room_id":          ev.get("room_id").cloned().unwrap_or(Value::Null),
            "origin_server_ts": ev.get("origin_server_ts").cloned().unwrap_or(Value::Null),
            "content":          json!({}),
            "unsigned":         { "redacted_because": redaction_event.clone() }
        })
    }

    // AppState::set_pusher:start
    //   purpose: Upsert a pusher for (user_id, app_id, pushkey) — the per-(user,app,
    //            pushkey) identity the Matrix spec uses for pusher uniqueness.
    //            Called by POST /pushers/set when kind is non-null.
    //   input:  user_id, record — PusherRecord (app_id/pushkey duplicated inside the
    //           record for dispatch's convenience; the map key is the source of truth)
    //   output: Ok(()) on success; Err(String) on mutex poison
    //   sideEffects: inserts/overwrites into self.push.pushers
    // AppState::set_pusher:end
    pub fn set_pusher(&self, user_id: &str, record: PusherRecord) -> Result<(), String> {
        let mut guard = self.push.pushers.lock().map_err(|e| e.to_string())?;
        guard.insert(
            (
                user_id.to_string(),
                record.app_id.clone(),
                record.pushkey.clone(),
            ),
            record,
        );
        Ok(())
    }

    // AppState::delete_pusher:start
    //   purpose: Remove a pusher for (user_id, app_id, pushkey).
    //            Called by POST /pushers/set when kind is explicitly null (the
    //            Matrix-spec deletion signal).
    //   input:  user_id, app_id, pushkey — the composite key
    //   output: Ok(()) on success (no-op if absent); Err(String) on mutex poison
    //   sideEffects: removes from self.push.pushers
    // AppState::delete_pusher:end
    pub fn delete_pusher(&self, user_id: &str, app_id: &str, pushkey: &str) -> Result<(), String> {
        let mut guard = self.push.pushers.lock().map_err(|e| e.to_string())?;
        guard.remove(&(user_id.to_string(), app_id.to_string(), pushkey.to_string()));
        Ok(())
    }

    // AppState::pushers_for_user:start
    //   purpose: Return every pusher registered for `user_id` (used by GET /pushers
    //            and by dispatch_push to find where to notify a given joined member).
    //   input:  user_id — full MXID
    //   output: Vec<PusherRecord> (cloned); empty on mutex poison
    //   sideEffects: none
    // AppState::pushers_for_user:end
    pub fn pushers_for_user(&self, user_id: &str) -> Vec<PusherRecord> {
        let Ok(guard) = self.push.pushers.lock() else {
            return Vec::new();
        };
        guard
            .iter()
            .filter(|((uid, _, _), _)| uid == user_id)
            .map(|(_, rec)| rec.clone())
            .collect()
    }

    // AppState::users_sharing_room_with:start
    //   purpose: Return the set of user_ids who are joined members of at least one
    //            room that `user_id` is ALSO a joined member of — the simplest
    //            correct approximation of "should this user's key changes be pushed
    //            to me" (Matrix requires this for /sync's device_lists.changed).
    //            SEAM: the spec-precise refinement scopes this to ENCRYPTED rooms
    //            only (rooms with an m.room.encryption state event); this server
    //            does not special-case that event type yet, so the coarser
    //            "any shared room" superset is used — safe (a client just runs an
    //            extra harmless keys/query) but not minimal.
    //            `user_id` itself is never included in the result.
    //   input:  user_id — the caller's full MXID
    //   output: HashSet<String> of other users' MXIDs; empty on mutex poison
    //   sideEffects: none (read-only scan of room_state)
    // AppState::users_sharing_room_with:end
    pub fn users_sharing_room_with(&self, user_id: &str) -> HashSet<String> {
        let mut result = HashSet::new();
        let Ok(guard) = self.room_state.lock() else {
            return result;
        };

        for events in guard.values() {
            let caller_is_member = events.iter().any(|ev| {
                ev.event_type == "m.room.member"
                    && ev.state_key == user_id
                    && ev.content.get("membership").and_then(|v| v.as_str()) == Some("join")
            });
            if !caller_is_member {
                continue;
            }
            for ev in events {
                if ev.event_type == "m.room.member"
                    && ev.state_key != user_id
                    && ev.content.get("membership").and_then(|v| v.as_str()) == Some("join")
                {
                    result.insert(ev.state_key.clone());
                }
            }
        }
        result
    }

    // AppState::append_room_timeline:start
    //   purpose: Append one client event to a room's timeline at a freshly minted
    //            stream_pos, applying the retention cap (timeline_max_events) when
    //            configured. This is the SINGLE chokepoint for timeline growth so
    //            the cap is honoured regardless of which write path produced the
    //            event (local send, state change, cluster merge, catch-up, replay).
    //            Phase 1 GC: trims oldest-first when over cap, mirroring the
    //            drain_to_device since-token retain pattern. No-op on mutex poison
    //            (best-effort, never panics on a poisoned lock).
    //   input:  room_id — target room; ev — the client_event JSON to append
    //   output: the stream_pos assigned to this event
    //   sideEffects: increments stream_pos; pushes (pos, ev) onto room_timeline;
    //                may drain the oldest entries if over cap
    // AppState::append_room_timeline:end
    // AppState::collect_room_log:start
    //   purpose: Apply the RoomLog GC cap to one room: keep roughly the newest
    //            `roomlog_max_events` events and collect everything older, raising the
    //            room's depth watermark so no peer can put them back.
    //
    //            "Roughly" because the cut is by depth, not by count: events sharing a
    //            depth (concurrent branches) are collected together, so a run may drop
    //            a few more than strictly necessary. Cutting mid-depth would be worse —
    //            it would leave a partial generation whose siblings could still return.
    //
    //            No-op when the cap is 0 (the default) or the log is under it, so this
    //            is cheap enough to call after every insert.
    //
    //            IRREVERSIBLE and CLUSTER-WIDE: the watermark propagates on the next
    //            delta and every peer adopts it. This is deletion, not trimming — a
    //            client can no longer page back past it anywhere in the cluster.
    //   input:  room_id
    //   output: number of events collected (0 if the cap is off or not reached)
    //   sideEffects: removes PDUs from the room's RoomLog; raises its watermark
    // AppState::collect_room_log:end
    pub fn collect_room_log(&self, room_id: &str) -> usize {
        self.collect_room_log_to(room_id, self.roomlog_max_events)
    }

    // AppState::collect_room_log_to:start
    //   purpose: collect_room_log with the cap passed in rather than read from config.
    //            Exists so the collection path can be exercised without setting a
    //            process-global environment variable, which parallel tests cannot do
    //            safely. Production calls it through collect_room_log.
    //   input:  room_id; cap — how many events to keep (0 disables collection)
    //   output: number of events collected
    //   sideEffects: as collect_room_log
    // AppState::collect_room_log_to:end
    pub fn collect_room_log_to(&self, room_id: &str, cap: usize) -> usize {
        if cap == 0 {
            return 0;
        }
        let Ok(mut rooms) = self.rooms.lock() else {
            return 0;
        };
        let Some(log) = rooms.get_mut(room_id) else {
            return 0;
        };
        if log.len() <= cap {
            return 0;
        }
        // Cut below the depth of the newest event we intend to drop.
        let mut depths: Vec<u64> = log.ordered().iter().map(|p| p.depth).collect();
        depths.sort_unstable();
        let cut = depths[depths.len() - cap - 1];
        let dropped = log.collect_below(cut);
        if dropped == 0 {
            return 0;
        }
        let survivors: std::collections::HashSet<String> =
            log.ordered().iter().map(|p| p.event_id.clone()).collect();
        drop(rooms);

        // Persist the decision and shrink the journal to match. Memory is bounded by
        // the watermark alone; disk needs the journal rewritten, and a restart needs
        // the marker or replay would undo the whole thing.
        if let Some(dir) = self.persist.data_dir.clone() {
            crate::persist::persist_room_gc(&dir, room_id, cut);
            crate::persist::prune_room_journal(&dir, room_id, &survivors);
            crate::persist::compact_room_pdumeta(&dir, room_id, &survivors);
        }
        dropped
    }

    // AppState::room_log_depth:start
    //   purpose: How many events the room's RoomLog holds right now. This is the
    //            UNTRIMMED depth: room_timeline may hold fewer because
    //            timeline_max_events drained the oldest, and the difference is
    //            exactly the tail a client has to backfill.
    //   input:  room_id
    //   output: number of PDUs in the RoomLog (0 if the room or log is absent)
    //   sideEffects: none (locks rooms, then releases)
    // AppState::room_log_depth:end
    pub fn room_log_depth(&self, room_id: &str) -> usize {
        let Ok(rooms) = self.rooms.lock() else {
            return 0;
        };
        rooms.get(room_id).map(|log| log.len()).unwrap_or(0)
    }

    // AppState::dropped_head_events:start
    //   purpose: Rebuild the client-event JSON for the OLDEST events that the
    //            timeline cap already drained, so /rooms/{id}/messages can serve
    //            them and a client holding sync's prev_batch can backfill the
    //            hole. The RoomLog keeps them (only roomlog_max_events deletes,
    //            and that is irreversible by design), so the data is present —
    //            it was simply unreachable, because /messages paginated over the
    //            already-trimmed projection and told the client nothing.
    //
    //            A Pdu carries the request body as raw JSON bytes (see
    //            routes/send.rs PDU construction: kind = event type, content =
    //            raw body, ts = wall-clock ms), which is everything the client
    //            event JSON needs, so the reconstruction is exact rather than
    //            approximate.
    //
    //            State events (m.room.* membership and the room's own state) are
    //            skipped: /sync delivers them under state.events, not the timeline,
    //            and /messages has always filtered them out.
    //   input:  room_id, stop_at_event_id — walk stops at this event (the projection's
    //            oldest); None walks the whole log. Returns the non-state events BEFORE it.
    //   output: Vec of client-event JSON values, chronological
    //   sideEffects: none (locks rooms, then releases)
    // AppState::dropped_head_events:end
    pub fn dropped_head_events(
        &self,
        room_id: &str,
        stop_at_event_id: Option<&str>,
    ) -> Vec<serde_json::Value> {
        let Ok(rooms) = self.rooms.lock() else {
            return Vec::new();
        };
        let Some(log) = rooms.get(room_id) else {
            return Vec::new();
        };
        Self::dropped_head_events_from(log, stop_at_event_id)
    }

    // AppState::dropped_head_events_from:start
    //   purpose: The same walk, but over a RoomLog the caller ALREADY holds locked.
    //            It exists because routes::sync::build_join_rooms keeps
    //            `state.rooms` locked across the whole response: re-locking it
    //            from there self-deadlocks (std::sync::Mutex is not reentrant),
    //            and /sync then hangs forever instead of failing. So the locked
    //            variant is for /messages, and this one for the sync path.
    //   input:  log — &RoomLog already under the caller's guard
    //            stop_at_event_id — walk stops at this event (the projection's oldest)
    //   output: Vec of client-event JSON values, chronological
    //   sideEffects: none
    // AppState::dropped_head_events_from:end
    pub fn dropped_head_events_from(
        log: &crate::substrate::matrix_events::RoomLog,
        stop_at_event_id: Option<&str>,
    ) -> Vec<serde_json::Value> {
        let mut out = Vec::new();
        for pdu in log.ordered() {
            // The projection's OLDEST event is where the tail begins: everything
            // before it in the log is what the cap already dropped. Bounding the
            // walk by that event — rather than by a count — is what keeps this
            // from returning the projection's own events a second time, which is
            // what made the first attempt build a duplicated /messages page.
            if stop_at_event_id == Some(pdu.event_id.as_str()) {
                break;
            }
            if pdu.kind.starts_with("m.room.") && !pdu.kind.starts_with("m.room.message") {
                continue;
            }
            let content = serde_json::from_slice::<serde_json::Value>(&pdu.content)
                .unwrap_or(serde_json::Value::Null);
            out.push(serde_json::json!({
                "type": pdu.kind,
                "event_id": pdu.event_id,
                "sender": pdu.sender,
                "room_id": pdu.room_id,
                "origin_server_ts": pdu.ts,
                "content": content,
            }));
        }
        out
    }

    pub fn append_room_timeline(&self, room_id: &str, ev: Value) -> u64 {
        use std::sync::atomic::Ordering;
        let mut ev = ev;
        // Recording happens here, before the timeline lock, so that a redaction takes
        // effect on every path that reaches this chokepoint — including journal replay,
        // which is what makes a redaction survive a restart. The redactions lock is
        // taken and released before room_timeline is acquired; never nest them.
        if let Some(target) = Self::redaction_target(&mut ev) {
            if let Err(e) = self.mark_redacted(&target, ev.clone()) {
                eprintln!("[matrix-hs] redaction of {target}: {e}");
            }
        }
        let pos = self.stream_pos.fetch_add(1, Ordering::SeqCst);
        if let Ok(mut rt) = self.room_timeline.lock() {
            let v = rt.entry(room_id.to_string()).or_default();
            v.push((pos, ev));
            let cap = self.timeline_max_events;
            if cap > 0 && v.len() > cap {
                v.drain(0..v.len() - cap);
            }
        }
        pos
    }

    // AppState::apply_remote_state_event:start
    //   purpose: Merge one remote room-state event into local room_state using
    //            Last-Writer-Wins keyed by (room_id, event_type, state_key), with a
    //            DETERMINISTIC tiebreak on (origin_server_ts, event_id) — mirrors the
    //            (ts, node_id) tiebreak pattern in crate::substrate::barrier::reconcile and the
    //            (ts, node) ordering in crate::substrate::crdt::LwwRegister::set, adapted here to
    //            (ts, event_id) since state events (not nodes) are the units being
    //            compared. See routes/room_state.rs module header for the full
    //            cross-node state-replication design and its explicit non-guarantees
    //            (no auth-chain state resolution, no power-level enforcement).
    //            Idempotent: redelivering the SAME event_id is a no-op (the tuple
    //            compare is strict '>', never '>=', so an identical resend never
    //            re-beats itself). Commutative/order-independent: two nodes applying
    //            the same set of concurrent writes, in ANY order, converge to the
    //            same winner because the comparison is a pure function of each
    //            candidate's own (ts, event_id) — never of arrival order or which
    //            node is doing the comparing.
    //   input:  ev — the remote StateEvent to (possibly) apply
    //   output: Ok(true) if `ev` became the new winner for its (event_type,state_key)
    //           slot (applied — room_state/room_timeline mutated); Ok(false) if an
    //           existing entry already wins (no-op, both content and the underlying
    //           maps are left untouched); Err(String) on mutex poison
    //   sideEffects: on Ok(true): ensures rooms/room_state/room_timeline entries exist
    //                for ev.room_id (ensure_room_state); replaces the
    //                (event_type,state_key) entry in room_state; appends a new
    //                room_timeline entry at a freshly minted stream_pos (so
    //                incremental /sync and sliding-sync observe the change); persists
    //                the event to the room journal (best-effort, no-op unless
    //                MATRIX_HS_DATA_DIR is set). Does NOT call notify.notify_waiters()
    //                — batched by the caller (drain_cluster_state) after processing
    //                all pending deltas.
    // AppState::apply_remote_state_event:end
    // AppState::may_set_state:start
    //   purpose: Receive-side authorisation for a remote state write: per the room's
    //            CURRENT power_levels, is this sender allowed to set this state?
    //
    //            Without it, LWW alone decides, and "whoever wrote last wins" means
    //            any node can rewrite any membership or any power_levels in any room
    //            it can reach — the single largest hole in sharing a room with a
    //            second operator. This closes it without adopting Matrix state
    //            resolution v2: it is a check against current state, not an
    //            auth-chain walk, and it does not attempt to re-resolve conflicts.
    //
    //            Deliberately NOT enforced:
    //              - join_rules / invite state (a join is allowed on membership
    //                grounds here; who may join at all is a separate gate this
    //                server has never had),
    //              - per-transition membership rules (kick vs ban vs invite all use
    //                the ordinary state level rather than their own),
    //              - the full old-vs-new comparison Matrix does on power_levels.
    //
    //   input:  current — the room's existing state events; ev — the incoming event
    //   output: true if the write is allowed
    //   sideEffects: none (pure — takes no locks, so callers may hold room_state)
    // AppState::may_set_state:end
    // AppState::hlc_now:start
    //   purpose: Next hybrid-logical-clock value, in milliseconds — the timestamp put
    //            on state events and therefore the LWW ordering key.
    //
    //            Replaces two things that were not clocks. Room state writes used
    //            `stream_pos * 1000`, a node-LOCAL event counter: two nodes' values
    //            were not comparable at all, a busier node won every race regardless
    //            of when anything happened, and because createRoom used the wall clock
    //            (~1.7e12) while later edits used the counter (~1e4), an edit could
    //            never beat the room's own creation state on a peer — a rename applied
    //            locally and silently lost everywhere else.
    //
    //            max(wall, last+1) keeps it monotonic and never behind real time, and
    //            hlc_observe pushes it past anything a peer has sent, so "later"
    //            follows causality rather than clock quality.
    //   input:  none
    //   output: a strictly increasing millisecond value
    //   sideEffects: advances self.hlc
    // AppState::hlc_now:end
    pub fn hlc_now(&self) -> u64 {
        use std::sync::atomic::Ordering;
        let wall = now_ms();
        loop {
            let last = self.hlc.load(Ordering::SeqCst);
            let next = wall.max(last + 1);
            if self
                .hlc
                .compare_exchange(last, next, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return next;
            }
        }
    }

    // AppState::hlc_observe:start
    //   purpose: Take a peer's timestamp into account so our next one is later. This
    //            is the half that makes the clock hybrid rather than merely local.
    //
    //            Bounded on purpose: a value more than HLC_MAX_DRIFT_MS ahead of our
    //            wall clock is ignored rather than adopted. Without that bound one
    //            node with a badly wrong clock — a dead RTC reading year 2099 — would
    //            drag every node's timestamps with it, permanently, and nothing later
    //            could ever win an LWW race again.
    //   input:  remote — origin_server_ts from a peer's state event
    //   output: none
    //   sideEffects: may advance self.hlc
    // AppState::hlc_observe:end
    pub fn hlc_observe(&self, remote: u64) {
        use std::sync::atomic::Ordering;
        let wall = now_ms();
        if remote > wall.saturating_add(HLC_MAX_DRIFT_MS) {
            eprintln!(
                "[matrix-hs] hlc: ignoring peer timestamp {remote}, more than \
                 {HLC_MAX_DRIFT_MS} ms ahead of local time {wall} — a peer clock this \
                 far out would otherwise freeze ordering for everyone"
            );
            return;
        }
        self.hlc.fetch_max(remote, Ordering::SeqCst);
    }

    pub fn may_set_state(current: &[StateEvent], ev: &StateEvent) -> bool {
        let Some(pl) = current
            .iter()
            .find(|e| e.event_type == "m.room.power_levels" && e.state_key.is_empty())
        else {
            // No power_levels known for this room yet, so there is nothing to judge
            // against. Refusing here would make rooms unreplicable — a room's own
            // power_levels event would be the first thing turned away.
            return true;
        };
        let c = &pl.content;
        let level_of = |user: &str| -> i64 {
            c.get("users")
                .and_then(|u| u.get(user))
                .and_then(|v| v.as_i64())
                .or_else(|| c.get("users_default").and_then(|v| v.as_i64()))
                .unwrap_or(0)
        };
        let sender_level = level_of(&ev.sender);

        // Managing your OWN membership is self-service. Without this a remote join
        // could never land: a joining user sits at users_default (0), far under
        // state_default (50). Acting on someone ELSE's membership falls through to
        // the ordinary check below.
        if ev.event_type == "m.room.member" && ev.state_key == ev.sender {
            return true;
        }

        let required = c
            .get("events")
            .and_then(|e| e.get(&ev.event_type))
            .and_then(|v| v.as_i64())
            .or_else(|| c.get("state_default").and_then(|v| v.as_i64()))
            .unwrap_or(50);
        if sender_level < required {
            return false;
        }

        // Escalation. The level check above only stops a low-power sender from
        // editing state; this is what stops one from promoting itself first and then
        // editing everything legitimately.
        if ev.event_type == "m.room.power_levels" && ev.state_key.is_empty() {
            let grants_above_self = ev
                .content
                .get("users")
                .and_then(|u| u.as_object())
                .map(|users| {
                    users
                        .values()
                        .any(|lvl| lvl.as_i64().unwrap_or(0) > sender_level)
                })
                .unwrap_or(false);
            let default_above_self = ev
                .content
                .get("users_default")
                .and_then(|v| v.as_i64())
                .is_some_and(|d| d > sender_level);
            if grants_above_self || default_above_self {
                return false;
            }
        }

        true
    }

    pub fn apply_remote_state_event(&self, ev: StateEvent) -> Result<bool, String> {
        self.ensure_room_state(&ev.room_id);
        // Learn from the peer's clock before anything else, so our next write is
        // ordered after theirs even if our own wall clock lags.
        self.hlc_observe(ev.origin_server_ts);

        let winner_json = {
            let mut rs = self.room_state.lock().map_err(|e| e.to_string())?;
            let room_vec = rs.entry(ev.room_id.clone()).or_default();

            // Authorise BEFORE the LWW compare. Winning on timestamp is not
            // permission: a node that may not set this state must not set it however
            // recent its clock claims to be.
            if !Self::may_set_state(room_vec, &ev) {
                eprintln!(
                    "[matrix-hs] DENY state {} (state_key {:?}) in {} from {}: \
                     insufficient power level",
                    ev.event_type, ev.state_key, ev.room_id, ev.sender
                );
                return Ok(false);
            }

            let existing = room_vec
                .iter()
                .find(|e| e.event_type == ev.event_type && e.state_key == ev.state_key);

            let beats = match existing {
                None => true,
                Some(cur) => {
                    (ev.origin_server_ts, ev.event_id.as_str())
                        > (cur.origin_server_ts, cur.event_id.as_str())
                }
            };

            if !beats {
                return Ok(false);
            }

            room_vec.retain(|e| !(e.event_type == ev.event_type && e.state_key == ev.state_key));
            room_vec.push(ev.clone());

            json!({
                "event_id":         ev.event_id.clone(),
                "type":             ev.event_type.clone(),
                "state_key":        ev.state_key.clone(),
                "sender":           ev.sender.clone(),
                "room_id":          ev.room_id.clone(),
                "origin_server_ts": ev.origin_server_ts,
                "content":          ev.content.clone()
            })
        };

        {
            self.persist_room_event(&ev.room_id, &winner_json);
            self.append_room_timeline(&ev.room_id, winner_json);
        }

        Ok(true)
    }

    // ── E2EE key backup (/room_keys) ──────────────────────────────────────────

    // AppState::create_room_key_version:start
    //   purpose: Mint a new backup version for `user_id` and store its metadata.
    //            Version numbers are minted from room_key_backup_seq — monotonically
    //            increasing per user, starting at "1", never reused (even across a
    //            delete). Sets this new version as the user's current version.
    //   input:  user_id — full MXID; algorithm, auth_data — opaque JSON as supplied
    //           by the client (stored exactly as given, never validated/decrypted)
    //   output: Ok(version_string) on success; Err(String) on mutex poison
    //   sideEffects: mutates room_key_backup_seq, room_key_versions,
    //                room_key_current_version
    // AppState::create_room_key_version:end
    pub fn create_room_key_version(
        &self,
        user_id: &str,
        algorithm: Value,
        auth_data: Value,
    ) -> Result<String, String> {
        let version = {
            let mut seq = self
                .e2ee
                .room_key_backup_seq
                .lock()
                .map_err(|e| e.to_string())?;
            let n = seq.entry(user_id.to_string()).or_insert(0);
            *n += 1;
            n.to_string()
        };

        {
            let mut versions = self
                .e2ee
                .room_key_versions
                .lock()
                .map_err(|e| e.to_string())?;
            versions.insert(
                (user_id.to_string(), version.clone()),
                RoomKeyBackupVersion {
                    algorithm,
                    auth_data,
                    etag: 0,
                },
            );
        }
        {
            let mut cur = self
                .e2ee
                .room_key_current_version
                .lock()
                .map_err(|e| e.to_string())?;
            cur.insert(user_id.to_string(), version.clone());
        }
        Ok(version)
    }

    // AppState::current_room_key_version:start
    //   purpose: Resolve `user_id`'s current backup version (the target of GET
    //            .../room_keys/version with no version in the path).
    //   input:  user_id — full MXID
    //   output: Some(version_string) if one is set (and has not been deleted);
    //           None if the user never created a backup, or their current version
    //           was deleted and no new one has been created since
    //   sideEffects: none
    // AppState::current_room_key_version:end
    pub fn current_room_key_version(&self, user_id: &str) -> Option<String> {
        self.e2ee
            .room_key_current_version
            .lock()
            .ok()?
            .get(user_id)
            .cloned()
    }

    // AppState::get_room_key_version:start
    //   purpose: Look up the stored (algorithm, auth_data, etag, count) for
    //            (user_id, version). count is computed on the fly from
    //            room_key_data (number of stored sessions across all rooms).
    //   input:  user_id, version
    //   output: Some((algorithm, auth_data, etag, count)) if the version exists;
    //           None if absent or mutex poisoned
    //   sideEffects: none
    // AppState::get_room_key_version:end
    pub fn get_room_key_version(
        &self,
        user_id: &str,
        version: &str,
    ) -> Option<(Value, Value, u64, u64)> {
        let meta = {
            let versions = self.e2ee.room_key_versions.lock().ok()?;
            versions
                .get(&(user_id.to_string(), version.to_string()))?
                .clone()
        };
        let count = self.room_key_count(user_id, version);
        Some((meta.algorithm, meta.auth_data, meta.etag, count))
    }

    // AppState::update_room_key_version:start
    //   purpose: Update algorithm and/or auth_data of an existing backup version
    //            in place. Fields omitted by the caller (None) are left unchanged.
    //   input:  user_id, version; algorithm — Some(new value) to replace, None to
    //           keep; auth_data — same
    //   output: Ok(true) if the version existed and was updated; Ok(false) if the
    //           version does not exist (caller should 404); Err(String) on mutex
    //           poison
    //   sideEffects: mutates room_key_versions
    // AppState::update_room_key_version:end
    pub fn update_room_key_version(
        &self,
        user_id: &str,
        version: &str,
        algorithm: Option<Value>,
        auth_data: Option<Value>,
    ) -> Result<bool, String> {
        let mut versions = self
            .e2ee
            .room_key_versions
            .lock()
            .map_err(|e| e.to_string())?;
        let Some(meta) = versions.get_mut(&(user_id.to_string(), version.to_string())) else {
            return Ok(false);
        };
        if let Some(a) = algorithm {
            meta.algorithm = a;
        }
        if let Some(ad) = auth_data {
            meta.auth_data = ad;
        }
        Ok(true)
    }

    // AppState::delete_room_key_version:start
    //   purpose: Delete a backup version's metadata and all of its stored keys.
    //            If this was the user's current version, clears current (Matrix-
    //            correct: the client must create a new version to resume backing
    //            up — a deleted version is never silently re-adopted as current).
    //   input:  user_id, version
    //   output: Ok(true) if the version existed and was deleted; Ok(false) if it
    //           did not exist (caller should 404); Err(String) on mutex poison
    //   sideEffects: mutates room_key_versions, room_key_data,
    //                room_key_current_version
    // AppState::delete_room_key_version:end
    pub fn delete_room_key_version(&self, user_id: &str, version: &str) -> Result<bool, String> {
        let key = (user_id.to_string(), version.to_string());
        let existed = {
            let mut versions = self
                .e2ee
                .room_key_versions
                .lock()
                .map_err(|e| e.to_string())?;
            versions.remove(&key).is_some()
        };
        if !existed {
            return Ok(false);
        }
        {
            let mut data = self.e2ee.room_key_data.lock().map_err(|e| e.to_string())?;
            data.remove(&key);
        }
        {
            let mut cur = self
                .e2ee
                .room_key_current_version
                .lock()
                .map_err(|e| e.to_string())?;
            if cur.get(user_id).map(|v| v.as_str()) == Some(version) {
                cur.remove(user_id);
            }
        }
        Ok(true)
    }

    // AppState::room_key_count:start
    //   purpose: Count the total number of stored sessions across all rooms for
    //            (user_id, version) — the "count" field in backup-version and
    //            keys-mutation responses.
    //   input:  user_id, version
    //   output: u64 total session count (0 if the version has no stored keys, or
    //           does not exist, or on mutex poison)
    //   sideEffects: none
    // AppState::room_key_count:end
    pub fn room_key_count(&self, user_id: &str, version: &str) -> u64 {
        let Ok(data) = self.e2ee.room_key_data.lock() else {
            return 0;
        };
        let Some(rooms) = data.get(&(user_id.to_string(), version.to_string())) else {
            return 0;
        };
        rooms.values().map(|sessions| sessions.len() as u64).sum()
    }

    // AppState::bump_room_key_etag:start
    //   purpose: Increment and return the etag counter for (user_id, version) —
    //            called after any mutation (put or delete) to that version's
    //            stored keys, so the caller's response reflects the new etag.
    //   input:  user_id, version
    //   output: Ok(Some(new_etag)) if the version exists; Ok(None) if it does not
    //           (caller should 404); Err(String) on mutex poison
    //   sideEffects: mutates room_key_versions[(user_id,version)].etag
    // AppState::bump_room_key_etag:end
    pub fn bump_room_key_etag(&self, user_id: &str, version: &str) -> Result<Option<u64>, String> {
        let mut versions = self
            .e2ee
            .room_key_versions
            .lock()
            .map_err(|e| e.to_string())?;
        let Some(meta) = versions.get_mut(&(user_id.to_string(), version.to_string())) else {
            return Ok(None);
        };
        meta.etag += 1;
        Ok(Some(meta.etag))
    }

    // AppState::version_exists:start
    //   purpose: Check whether (user_id, version) has backup-version metadata —
    //            used to 404 the keys PUT/GET/DELETE endpoints when the caller
    //            references a version that was never created (or was deleted).
    //   input:  user_id, version
    //   output: bool (false on mutex poison)
    //   sideEffects: none
    // AppState::version_exists:end
    pub fn room_key_version_exists(&self, user_id: &str, version: &str) -> bool {
        self.e2ee
            .room_key_versions
            .lock()
            .map(|v| v.contains_key(&(user_id.to_string(), version.to_string())))
            .unwrap_or(false)
    }

    // AppState::put_room_key_session:start
    //   purpose: Store (overwrite) one session's KeyBackupData for
    //            (user_id, version, room_id, session_id). Does NOT validate the
    //            version exists — callers (routes/room_keys.rs) check
    //            room_key_version_exists first and 404 before calling this.
    //   input:  user_id, version, room_id, session_id; data — opaque KeyBackupData
    //           JSON blob exactly as the client supplied it
    //   output: Ok(()) on success; Err(String) on mutex poison
    //   sideEffects: mutates room_key_data[(user_id,version)][room_id][session_id]
    // AppState::put_room_key_session:end
    pub fn put_room_key_session(
        &self,
        user_id: &str,
        version: &str,
        room_id: &str,
        session_id: &str,
        data: Value,
    ) -> Result<(), String> {
        let mut store = self.e2ee.room_key_data.lock().map_err(|e| e.to_string())?;
        store
            .entry((user_id.to_string(), version.to_string()))
            .or_default()
            .entry(room_id.to_string())
            .or_default()
            .insert(session_id.to_string(), data);
        Ok(())
    }

    // AppState::get_room_key_data:start
    //   purpose: Read stored backup data for (user_id, version), optionally scoped
    //            to a room and/or session.
    //   input:  user_id, version; room_id — Some to scope to one room, None for all;
    //           session_id — Some to scope to one session (requires room_id Some),
    //           None for all sessions in scope
    //   output: serde_json::Value shaped per Matrix spec:
    //             - room_id+session_id given: the single KeyBackupData blob, or
    //               Value::Null if absent
    //             - room_id given, session_id None: {"sessions": {session_id: data}}
    //             - neither given: {"rooms": {room_id: {"sessions": {session_id: data}}}}
    //   sideEffects: none
    // AppState::get_room_key_data:end
    pub fn get_room_key_data(
        &self,
        user_id: &str,
        version: &str,
        room_id: Option<&str>,
        session_id: Option<&str>,
    ) -> Value {
        let Ok(store) = self.e2ee.room_key_data.lock() else {
            return json!({ "rooms": {} });
        };
        let Some(rooms) = store.get(&(user_id.to_string(), version.to_string())) else {
            return match (room_id, session_id) {
                (Some(_), Some(_)) => Value::Null,
                (Some(_), None) => json!({ "sessions": {} }),
                (None, _) => json!({ "rooms": {} }),
            };
        };

        match (room_id, session_id) {
            (Some(rid), Some(sid)) => rooms
                .get(rid)
                .and_then(|sessions| sessions.get(sid))
                .cloned()
                .unwrap_or(Value::Null),
            (Some(rid), None) => {
                let sessions = rooms.get(rid).cloned().unwrap_or_default();
                json!({ "sessions": Value::Object(sessions.into_iter().collect()) })
            }
            (None, _) => {
                let mut rooms_obj = serde_json::Map::new();
                for (rid, sessions) in rooms.iter() {
                    let sessions_obj: serde_json::Map<String, Value> = sessions
                        .iter()
                        .map(|(sid, v)| (sid.clone(), v.clone()))
                        .collect();
                    rooms_obj.insert(
                        rid.clone(),
                        json!({ "sessions": Value::Object(sessions_obj) }),
                    );
                }
                json!({ "rooms": Value::Object(rooms_obj) })
            }
        }
    }

    // AppState::delete_room_key_data:start
    //   purpose: Delete stored backup data for (user_id, version), scoped exactly
    //            like get_room_key_data (room_id/session_id optional).
    //   input:  user_id, version, room_id, session_id — same scoping rules as
    //           get_room_key_data
    //   output: Ok(()) on success; Err(String) on mutex poison
    //   sideEffects: mutates room_key_data (removes the matching sub-tree)
    // AppState::delete_room_key_data:end
    pub fn delete_room_key_data(
        &self,
        user_id: &str,
        version: &str,
        room_id: Option<&str>,
        session_id: Option<&str>,
    ) -> Result<(), String> {
        let mut store = self.e2ee.room_key_data.lock().map_err(|e| e.to_string())?;
        let Some(rooms) = store.get_mut(&(user_id.to_string(), version.to_string())) else {
            return Ok(());
        };
        match (room_id, session_id) {
            (Some(rid), Some(sid)) => {
                if let Some(sessions) = rooms.get_mut(rid) {
                    sessions.remove(sid);
                }
            }
            (Some(rid), None) => {
                rooms.remove(rid);
            }
            (None, _) => {
                rooms.clear();
            }
        }
        Ok(())
    }

    // ── Cross-signing (POST /keys/device_signing/upload, keys/signatures/upload) ──

    // AppState::set_cross_signing_keys:start
    //   purpose: Store (overwrite) a user's cross-signing key set. Fields the
    //            client omitted are left unchanged (a client may re-upload just
    //            one key later); fields present overwrite the prior value.
    //            SEAM: real Matrix requires User-Interactive Auth for this upload —
    //            see routes/keys.rs post_device_signing_upload for the marked seam;
    //            this method itself performs no auth, it just stores.
    //   input:  user_id; master_key, self_signing_key, user_signing_key — each
    //           Some(blob) to set/replace, None to leave the existing value as-is
    //   output: Ok(()) on success; Err(String) on mutex poison
    //   sideEffects: mutates self.e2ee.cross_signing_keys[user_id]
    // AppState::set_cross_signing_keys:end
    pub fn set_cross_signing_keys(
        &self,
        user_id: &str,
        master_key: Option<Value>,
        self_signing_key: Option<Value>,
        user_signing_key: Option<Value>,
    ) -> Result<(), String> {
        let mut guard = self
            .e2ee
            .cross_signing_keys
            .lock()
            .map_err(|e| e.to_string())?;
        let entry = guard.entry(user_id.to_string()).or_default();
        if let Some(mk) = master_key {
            entry.master_key = Some(mk);
        }
        if let Some(ssk) = self_signing_key {
            entry.self_signing_key = Some(ssk);
        }
        if let Some(usk) = user_signing_key {
            entry.user_signing_key = Some(usk);
        }
        Ok(())
    }

    // AppState::get_cross_signing_keys:start
    //   purpose: Look up a user's stored cross-signing keys, for keys/query.
    //   input:  user_id
    //   output: Some(CrossSigningKeys) clone if the user has uploaded any; None if
    //           absent or mutex poisoned
    //   sideEffects: none
    // AppState::get_cross_signing_keys:end
    pub fn get_cross_signing_keys(&self, user_id: &str) -> Option<CrossSigningKeys> {
        self.e2ee
            .cross_signing_keys
            .lock()
            .ok()?
            .get(user_id)
            .cloned()
    }

    // AppState::store_cross_signature:start
    //   purpose: Store one cross-signature blob uploaded via keys/signatures/upload,
    //            keyed by (uploader_user_id, target_key_or_device_id). Opaque:
    //            stored exactly as supplied, not merged into device_keys or
    //            cross_signing_keys (see routes/keys.rs post_signatures_upload doc
    //            comment for the exact minimal scope of this MVP implementation).
    //   input:  user_id — the uploading/authenticated caller; target_id — the key_id
    //           or device_id the signature blob is attached to; blob — the JSON
    //           value for that target_id as supplied in the request body
    //   output: Ok(()) on success; Err(String) on mutex poison
    //   sideEffects: mutates self.e2ee.cross_signatures[(user_id, target_id)]
    // AppState::store_cross_signature:end
    pub fn store_cross_signature(
        &self,
        user_id: &str,
        target_id: &str,
        blob: Value,
    ) -> Result<(), String> {
        let mut guard = self
            .e2ee
            .cross_signatures
            .lock()
            .map_err(|e| e.to_string())?;
        guard.insert((user_id.to_string(), target_id.to_string()), blob);
        Ok(())
    }

    // AppState::merge_signature:start
    //   purpose: Fold a signatures blob uploaded via keys/signatures/upload into
    //            the stored object it targets, so keys/query naturally returns it
    //            embedded in the object's "signatures" map (closes the MVP gap
    //            documented in routes/keys.rs::post_signatures_upload — without
    //            this, cross-signing master keys never acquire their device
    //            signature, so clients' crypto identity stays forever unverified
    //            and matrix-dart-sdk's bootstrap cannot complete verification).
    //
    //            Target resolution mirrors the Matrix spec:
    //              1. target_id is a device_id  -> merge into device_keys[(user, target_id)]
    //              2. target_id is a cross-signing key's ed25519 pubkey -> merge into the
    //                 matching master/self_signing/user_signing key object
    //              3. neither matched -> no-op (the opaque store_cross_signature, still
    //                 called by the handler, keeps the blob durably for later/other uses)
    //   input:  user_id — owner of the key being signed; target_id — device_id or
    //           cross-signing pubkey; sigs — the "signatures" map from the uploaded blob
    //           (shape {signing_user_id: {ed25519:KEY: signature}})
    //   output: Ok always (unknown targets are silently ignored per spec)
    //   sideEffects: mutates self.e2ee.device_keys or self.e2ee.cross_signing_keys
    // AppState::merge_signature:end
    pub fn merge_signature(
        &self,
        user_id: &str,
        target_id: &str,
        sigs: &serde_json::Map<String, Value>,
    ) -> Result<(), String> {
        // Helper: merge `sigs` into obj["signatures"] (creating it if absent),
        // unioning per-signer key→sig entries without dropping existing ones.
        fn fold_sigs(obj: &mut Value, sigs: &serde_json::Map<String, Value>) {
            let Some(map) = obj.as_object_mut() else {
                return;
            };
            let existing = map
                .entry("signatures".to_string())
                .or_insert_with(|| Value::Object(serde_json::Map::new()));
            let Some(ex_map) = existing.as_object_mut() else {
                return;
            };
            for (signer, additions) in sigs {
                if let Some(add_map) = additions.as_object() {
                    let slot = ex_map
                        .entry(signer.clone())
                        .or_insert_with(|| Value::Object(serde_json::Map::new()));
                    if let Some(slot_map) = slot.as_object_mut() {
                        for (k, v) in add_map {
                            slot_map.insert(k.clone(), v.clone());
                        }
                    }
                }
            }
        }
        // Helper: does this cross-signing key object's `keys` map reference target_id?
        // keys shape: {"ed25519:<pub>": "<pub>"} — target_id is the bare <pub>.
        fn key_matches(obj: &Value, target_id: &str) -> bool {
            let Some(keys) = obj.get("keys").and_then(|v| v.as_object()) else {
                return false;
            };
            keys.values().any(|v| v.as_str() == Some(target_id))
                || keys.contains_key(&format!("ed25519:{target_id}"))
        }

        // 1. device_keys keyed by (user_id, device_id == target_id)?
        {
            let mut guard = self.e2ee.device_keys.lock().map_err(|e| e.to_string())?;
            if let Some(dk) = guard.get_mut(&(user_id.to_string(), target_id.to_string())) {
                fold_sigs(dk, sigs);
                return Ok(());
            }
        }
        // 2. cross_signing_keys: match target_id against master/self/user pubkeys.
        {
            let mut guard = self
                .e2ee
                .cross_signing_keys
                .lock()
                .map_err(|e| e.to_string())?;
            if let Some(csk) = guard.get_mut(user_id) {
                for obj in [
                    &mut csk.master_key,
                    &mut csk.self_signing_key,
                    &mut csk.user_signing_key,
                ]
                .into_iter()
                .flatten()
                {
                    if key_matches(obj, target_id) {
                        fold_sigs(obj, sigs);
                        return Ok(());
                    }
                }
            }
        }
        // 3. No match — spec says silently succeed (blob already kept opaquely).
        Ok(())
    }

    // ── Account data + room tags ─────────────────────────────────────────────

    // AppState::set_account_data_global:start
    //   purpose: Upsert one global account_data entry for a user.
    //            PUT /user/{userId}/account_data/{type} — content is stored exactly
    //            as supplied (opaque JSON blob per the Matrix spec).
    //   input:  user_id — full MXID; event_type — account data type
    //           (e.g. "m.direct"); content — opaque JSON value
    //   output: Ok(()) on success; Err(String) on mutex poison
    //   sideEffects: inserts/overwrites self.account_data.account_data_global[user_id][event_type]
    // AppState::set_account_data_global:end
    pub fn set_account_data_global(
        &self,
        user_id: &str,
        event_type: &str,
        content: Value,
    ) -> Result<(), String> {
        let mut guard = self
            .account_data
            .account_data_global
            .lock()
            .map_err(|e| e.to_string())?;
        guard
            .entry(user_id.to_string())
            .or_insert_with(HashMap::new)
            .insert(event_type.to_string(), content);
        drop(guard);
        // account_data.events is level-triggered (always the current full snapshot,
        // see account_data_global_events) so no stream_pos bump is needed for
        // correctness — but a client long-polling /sync must still be woken
        // immediately, or it silently sits until its timeout elapses before ever
        // seeing this change (matches the to_device/device_list wake pattern).
        self.notify.notify_waiters();
        Ok(())
    }

    // AppState::get_account_data_global:start
    //   purpose: Look up one global account_data entry for a user.
    //            GET /user/{userId}/account_data/{type} — 404 M_NOT_FOUND if absent
    //            (the route handler converts None accordingly).
    //   input:  user_id, event_type
    //   output: Ok(Some(content)) if set; Ok(None) if not set; Err on mutex poison
    //   sideEffects: none
    // AppState::get_account_data_global:end
    pub fn get_account_data_global(
        &self,
        user_id: &str,
        event_type: &str,
    ) -> Result<Option<Value>, String> {
        let guard = self
            .account_data
            .account_data_global
            .lock()
            .map_err(|e| e.to_string())?;
        Ok(guard.get(user_id).and_then(|m| m.get(event_type)).cloned())
    }

    // AppState::account_data_global_events:start
    //   purpose: Build the "events" array for /sync's top-level account_data block
    //            (and the sliding-sync account_data extension's "global" array):
    //            one {"type","content"} object per event_type this user has set.
    //   input:  user_id — full MXID
    //   output: Vec<Value> of {"type": .., "content": ..} objects; empty if the
    //           user has never set any global account data, or on mutex poison
    //   sideEffects: none
    // AppState::account_data_global_events:end
    pub fn account_data_global_events(&self, user_id: &str) -> Vec<Value> {
        let Ok(guard) = self.account_data.account_data_global.lock() else {
            return Vec::new();
        };
        let Some(map) = guard.get(user_id) else {
            return Vec::new();
        };
        map.iter()
            .map(|(event_type, content)| json!({ "type": event_type, "content": content }))
            .collect()
    }

    // AppState::set_account_data_room:start
    //   purpose: Upsert one per-room account_data entry for a user.
    //            PUT /user/{userId}/rooms/{roomId}/account_data/{type}.
    //   input:  user_id, room_id, event_type, content — opaque JSON value
    //   output: Ok(()) on success; Err(String) on mutex poison
    //   sideEffects: inserts/overwrites
    //                self.account_data.account_data_room[user_id][room_id][event_type]
    // AppState::set_account_data_room:end
    pub fn set_account_data_room(
        &self,
        user_id: &str,
        room_id: &str,
        event_type: &str,
        content: Value,
    ) -> Result<(), String> {
        let mut guard = self
            .account_data
            .account_data_room
            .lock()
            .map_err(|e| e.to_string())?;
        guard
            .entry(user_id.to_string())
            .or_insert_with(HashMap::new)
            .entry(room_id.to_string())
            .or_insert_with(HashMap::new)
            .insert(event_type.to_string(), content);
        drop(guard);
        // See set_account_data_global: wake /sync long-poll waiters immediately.
        self.notify.notify_waiters();
        Ok(())
    }

    // AppState::get_account_data_room:start
    //   purpose: Look up one per-room account_data entry for a user.
    //            GET /user/{userId}/rooms/{roomId}/account_data/{type} — 404
    //            M_NOT_FOUND if absent (handled by the route handler).
    //   input:  user_id, room_id, event_type
    //   output: Ok(Some(content)) if set; Ok(None) if not; Err on mutex poison
    //   sideEffects: none
    // AppState::get_account_data_room:end
    pub fn get_account_data_room(
        &self,
        user_id: &str,
        room_id: &str,
        event_type: &str,
    ) -> Result<Option<Value>, String> {
        let guard = self
            .account_data
            .account_data_room
            .lock()
            .map_err(|e| e.to_string())?;
        Ok(guard
            .get(user_id)
            .and_then(|rooms| rooms.get(room_id))
            .and_then(|m| m.get(event_type))
            .cloned())
    }

    // AppState::account_data_room_events:start
    //   purpose: Build the "events" array for one room's account_data block in
    //            /sync (rooms.join.{roomId}.account_data.events) and the
    //            sliding-sync account_data extension's "rooms" map — one
    //            {"type","content"} object per event_type set for this
    //            (user, room), PLUS a synthetic "m.tag" event when the user has
    //            any tags set on this room (per the Matrix spec, room tags are
    //            delivered to clients as an m.tag account_data event, not a
    //            separate sync block).
    //   input:  user_id, room_id
    //   output: Vec<Value> of {"type","content"} objects; empty if nothing set
    //           for this (user, room), or on mutex poison
    //   sideEffects: none
    // AppState::account_data_room_events:end
    pub fn account_data_room_events(&self, user_id: &str, room_id: &str) -> Vec<Value> {
        let mut events = Vec::new();

        if let Ok(guard) = self.account_data.account_data_room.lock() {
            if let Some(map) = guard.get(user_id).and_then(|rooms| rooms.get(room_id)) {
                for (event_type, content) in map.iter() {
                    events.push(json!({ "type": event_type, "content": content }));
                }
            }
        }

        if let Some(tags) = self.room_tags_for(user_id, room_id) {
            if !tags.is_empty() {
                events.push(json!({ "type": "m.tag", "content": { "tags": tags } }));
            }
        }

        events
    }

    // AppState::set_room_tag:start
    //   purpose: Upsert one room tag for a user (PUT
    //            .../rooms/{roomId}/tags/{tag}). content is stored opaquely
    //            (typically {"order": <f64>} per the Matrix spec, but any JSON
    //            object is accepted unvalidated).
    //   input:  user_id, room_id, tag, content — opaque JSON value
    //   output: Ok(()) on success; Err(String) on mutex poison
    //   sideEffects: inserts/overwrites self.account_data.room_tags[user_id][room_id][tag]
    // AppState::set_room_tag:end
    pub fn set_room_tag(
        &self,
        user_id: &str,
        room_id: &str,
        tag: &str,
        content: Value,
    ) -> Result<(), String> {
        let mut guard = self
            .account_data
            .room_tags
            .lock()
            .map_err(|e| e.to_string())?;
        guard
            .entry(user_id.to_string())
            .or_insert_with(HashMap::new)
            .entry(room_id.to_string())
            .or_insert_with(HashMap::new)
            .insert(tag.to_string(), content);
        Ok(())
    }

    // AppState::delete_room_tag:start
    //   purpose: Remove one room tag for a user (DELETE
    //            .../rooms/{roomId}/tags/{tag}).
    //   input:  user_id, room_id, tag
    //   output: Ok(true) if a tag was actually removed; Ok(false) if it was not
    //           present (still a 200 per the Matrix spec — delete is idempotent);
    //           Err(String) on mutex poison
    //   sideEffects: removes self.account_data.room_tags[user_id][room_id][tag] if present
    // AppState::delete_room_tag:end
    pub fn delete_room_tag(&self, user_id: &str, room_id: &str, tag: &str) -> Result<bool, String> {
        let mut guard = self
            .account_data
            .room_tags
            .lock()
            .map_err(|e| e.to_string())?;
        let removed = guard
            .get_mut(user_id)
            .and_then(|rooms| rooms.get_mut(room_id))
            .map(|tags| tags.remove(tag).is_some())
            .unwrap_or(false);
        Ok(removed)
    }

    // AppState::room_tags_for:start
    //   purpose: Return all tags a user has set on a room (GET
    //            .../rooms/{roomId}/tags, and account_data_room_events above for
    //            the synthetic m.tag sync event).
    //   input:  user_id, room_id
    //   output: Some(HashMap<tag, content>) — empty map if the room is known but
    //           has no tags; None only on mutex poison (callers treat that the
    //           same as "no tags")
    //   sideEffects: none
    // AppState::room_tags_for:end
    pub fn room_tags_for(&self, user_id: &str, room_id: &str) -> Option<HashMap<String, Value>> {
        let guard = self.account_data.room_tags.lock().ok()?;
        Some(
            guard
                .get(user_id)
                .and_then(|rooms| rooms.get(room_id))
                .cloned()
                .unwrap_or_default(),
        )
    }
}

// localpart:start
//   purpose: Normalise a client-supplied user identifier to its bare localpart.
//            Per the Matrix spec, an `m.id.user` login identifier's `user` field
//            MAY be either a bare localpart ("tester") or a full MXID
//            ("@tester:localhost") — servers must accept both.  matrix-nio /
//            matrix-commander send the full MXID; Element sends the localpart.
//            Normalising here (before minting tok_<localpart>) keeps every
//            downstream `tok_` → user_id derivation correct and prevents the
//            "@@tester:localhost:localhost" double-wrap.
//   input:  user — bare localpart or full MXID
//   output: &str — the localpart (leading '@' stripped, truncated at first ':')
//   sideEffects: none
// localpart:end
pub fn localpart(user: &str) -> &str {
    let no_at = user.strip_prefix('@').unwrap_or(user);
    no_at.split(':').next().unwrap_or(no_at)
}

// now_ms:start
//   purpose: Wall-clock milliseconds since the Unix epoch, for event origin_server_ts.
//            mrgd's Pdu layer is deliberately clock-free (ts injected by the
//            caller) so it stays deterministic/testable; the HTTP layer injects real
//            time here.  Real timestamps matter for clients: Element sorts and dates
//            the timeline by origin_server_ts — a 0/epoch value shows every message
//            at 1970-01-01 and misorders rooms.
//   input:  none
//   output: u64 — milliseconds since 1970-01-01T00:00:00Z (0 if the clock is before
//                 the epoch, which cannot happen in practice)
//   sideEffects: reads the system clock
// now_ms:end
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod to_device_delivery_test {
    use super::*;
    use serde_json::json;

    // Regression for the room_key-loss bug (reported by the E2EE audit against a
    // busy node): a to-device message enqueued at a stream_pos BELOW the receiver's
    // room-sync cursor must STILL be delivered. The old drain_to_device used the
    // room since token as the delivery ack, so on a node where room traffic had
    // already advanced the device's cursor past the message's pos, the message was
    // GC'd on first read — to_device.events stayed empty forever and the Olm room
    // key never arrived. The ack is now the per-device `delivered` watermark.
    #[test]
    fn to_device_enqueued_below_room_since_is_not_dropped() {
        let st = AppState::new();

        // Device's room sync has already advanced the shared counter past 100 —
        // e.g. the invite/{{createRoom}} sequence that precedes the room_key send.
        // Simulate that traffic so the to-device pos lands well below the cursor.
        st.stream_pos.store(1000, std::sync::atomic::Ordering::SeqCst);

        // Sender hands out the Olm session key AFTER the receiver's cursor already
        // passed this region — exactly the invite→send E2EE sequence.
        st.enqueue_to_device(
            "@bob:localhost",
            "BOBDEV",
            "@alice:localhost",
            "m.room_key",
            &json!({"session_id": "sess-1"}),
            "a:keys/:bob:BOBDEV",
        )
        .unwrap();

        // Receiver syncs whose since token already exceeds the message's pos (1001
        // vs. a cursor ≥ the enqueued pos). Old code dropped it here.
        let got = st
            .drain_to_device("@bob:localhost", "BOBDEV", Some(1000))
            .unwrap();
        assert_eq!(got.len(), 1, "message below the room cursor must still be delivered");
        assert_eq!(got[0]["type"], "m.room_key");
        assert_eq!(got[0]["sender"], "@alice:localhost");
        assert_eq!(got[0]["content"]["session_id"], "sess-1");
    }

    // Exactly-once-per-delivery retained: once drain hands an event back, a later
    // drain must not redeliver it (the pre-existing cross-node test's assertion).
    #[test]
    fn to_device_delivered_once_per_device() {
        let st = AppState::new();

        st.enqueue_to_device(
            "@bob:localhost",
            "BOBDEV",
            "@alice:localhost",
            "m.room_key",
            &json!({"session_id": "sess-2"}),
            "a:keys/:bob:BOBDEV",
        )
        .unwrap();

        let first = st
            .drain_to_device("@bob:localhost", "BOBDEV", None)
            .unwrap();
        assert_eq!(first.len(), 1);

        let second = st
            .drain_to_device("@bob:localhost", "BOBDEV", None)
            .unwrap();
        assert!(second.is_empty(), "must not redeliver after the first drain");
    }

    // Two devices of the same user have independent watermarks: delivering to one
    // must not suppress delivery to the other.
    #[test]
    fn to_device_watermarks_are_per_device() {
        let st = AppState::new();

        st.enqueue_to_device(
            "@bob:localhost",
            "BOBDEV1",
            "@alice:localhost",
            "m.room_key",
            &json!({"session_id": "sess-3"}),
            "a:keys/:bob:BOBDEV1",
        )
        .unwrap();
        st.enqueue_to_device(
            "@bob:localhost",
            "BOBDEV2",
            "@alice:localhost",
            "m.room_key",
            &json!({"session_id": "sess-3"}),
            "a:keys/:bob:BOBDEV2",
        )
        .unwrap();

        assert_eq!(st.drain_to_device("@bob:localhost", "BOBDEV1", None).unwrap().len(), 1);
        assert_eq!(st.drain_to_device("@bob:localhost", "BOBDEV2", None).unwrap().len(), 1);
    }
}
